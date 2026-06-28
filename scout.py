"""
Job Scout — Daily job search engine for Aaron

Architecture:
1. Ingestion (raw ATS + Adzuna + MaRS + Wellfound + Remotive, no filtering)
2. ATS normalization layer (canonical schema)
3. Objective filtering (SQL/Python + non-engineer rule)
4. Claude scoring (structured rubric)
5. Email digest for scores >= 8
"""

import os
import json
import hashlib
import logging
import time
import re
import smtplib
import requests
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

ADZUNA_APP_ID     = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY    = os.environ["ADZUNA_APP_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GMAIL_SENDER      = os.environ["GMAIL_SENDER"]
GMAIL_RECIPIENT   = os.environ["GMAIL_RECIPIENT"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]

SCORE_THRESHOLD = 8

SEEN_JOBS_FILE      = Path("data/seen_jobs.json")
SEEN_YC_SLUGS_FILE  = Path("data/seen_yc_slugs.json")

# ── YC category tags we care about ───────────────────────────────────────────

YC_RELEVANT_TAGS = {
    "gaming", "consumer", "education", "edtech", "health", "healthcare",
    "entertainment", "media", "fitness", "mental health", "marketplace",
    "productivity", "community", "sports", "creator economy", "social",
    "food", "food and beverage", "travel", "music", "art", "climate",
    "sustainability", "developer tools", "retail", "e-commerce",
}

# ── Manual company lists ──────────────────────────────────────────────────────

GREENHOUSE_COMPANIES = list(dict.fromkeys([
    # Gaming & entertainment
    "riotgames", "epicgames", "scopely", "niantic", "kabam",
    # Consumer / human-centered
    "duolingo", "headspace", "calm", "strava", "peloton",
    "bumble", "eventbrite", "etsy", "patreon", "khanacademy",
    "coursera", "figma",
    # Canadian / Toronto
    "wealthsimple", "hootsuite", "lightspeed", "d2l",
    # Other product companies
    "airtable", "mixpanel", "amplitude", "discord",
    "reddit", "instacart", "pinterest", "lyft", "robinhood",
    "databricks", "cloudflare", "stripe", "airbnb",
]))

LEVER_COMPANIES = [
    "naughtydog",
    "whoop", "oura", "faire",
    "benchsci", "sonder",
    "spotify", "netflix",
]

WORKABLE_COMPANIES = [
    "koho", "wattpad", "achievers", "ada-support",
    "typeform", "remote",
]

ASHBY_COMPANIES = [
    "notion", "linear", "ramp", "replit", "cohere",
    "perplexity", "cursor", "vanta", "watershed",
    "moonactive",
    "belong", "dayoneapp",
    "cohere",
    "properly",
]

# ── Wellfound role slugs to query ─────────────────────────────────────────────

WELLFOUND_ROLES = [
    "data-analyst",
    "product-analyst",
    "analytics-engineer",
    "data-scientist",
    "growth-analyst",
    "business-analyst",
]

# ── Remotive search terms ─────────────────────────────────────────────────────

REMOTIVE_SEARCHES = [
    "data analyst",
    "product analyst",
    "analytics engineer",
    "growth analyst",
    "business intelligence",
]

# ── Claude matching criteria ──────────────────────────────────────────────────

MATCHING_CRITERIA = """
You are evaluating job postings for Aaron, a Senior Product Data Analyst based in Toronto.

You will score each job on FOUR dimensions. Return ONLY valid JSON.

-------------------------
SCORING MODEL (TOTAL = 100)
-------------------------

1. Career & Work Quality — 30%
(trajectory + day-to-day analytical depth combined)

Question:
Is this the right kind of analytics role AND does it involve meaningful analytical work?

High (9–10):
- Product Analytics
- Growth / Monetization Analytics
- Decision Science
- Experimentation / A/B testing roles
- Data Science (product-focused)
- Analytics Engineering (embedded in product decisions)
- Strategy & Ops / BizOps (ONLY if SQL + real analytical ownership)
- Marketplace / user behavior analytics
- Pricing / revenue analytics (decision-driven)

Medium (5–8):
- Business Intelligence (mixed analysis + reporting)
- Marketing / Customer Analytics
- Operational analytics
- General Data Analyst roles

Low (0–4):
- Reporting-only roles
- Dashboard maintenance
- SQL ticket / extraction work
- Data QA / validation
- ETL / pipeline maintenance
- Junior / entry-level scoped roles

Key rule:
Both career trajectory AND analytical depth must be strong to score high.

-------------------------

2. Company Interest — 30%

Question:
Would I actually want to work at and represent this company/product?

High (9–10):
- Strong consumer products
- Gaming / entertainment / creator tools
- Education, healthcare, civic/urban systems (user-facing)
- Highly engaging, well-designed products
- Clear product value and user base

Medium (5–8):
- Strong B2B SaaS with clear value
- Developer tools / infrastructure platforms
- Fintech (depending on consumer vs backend focus)

Low (0–4):
- Vague enterprise SaaS
- Staffing / consulting firms
- Low-product-clarity companies

Key rule:
This is about personal interest and product appeal.

-------------------------

3. Impact — 20%

Question:
How meaningful is the work in terms of real-world or system-level effect?

High (9–10):
- Healthcare, mental health
- Education
- Civic systems (transportation, housing, government services)
- Marketplaces affecting real user outcomes
- Consumer systems shaping behavior at scale

Medium (5–8):
- Enterprise SaaS with operational importance
- Fintech infrastructure
- Developer tools and platforms

Low (0–4):
- Internal tooling
- Compliance/reporting-only systems
- Backend analytics with no downstream effect

Key rule:
Focus on real-world or system-level impact, not company branding.

-------------------------

4. Logistics & Compensation — 20%

Question:
Is this role practically viable?

High (9–10):
- Canada / Toronto / remote (Canada-eligible)
- Mid-level or senior roles
- Market-aligned compensation
- Full-time structured roles

Medium (5–8):
- Hybrid ambiguity
- Contract roles with strong upside
- Salary unclear but likely acceptable

Low (0–4):
- Internships / entry-level roles
- Relocation required outside Canada
- Staffing agencies
- Under-market compensation
- Unstable or unclear employment structure

Key rule:
This is a feasibility filter only.

-------------------------

OUTPUT FORMAT (STRICT)

Return ONLY JSON (Do not include markdown or backticks):
{
  "recommend": true or false,
  "score": 1-10,
  "breakdown": {
    "career_work_quality": 0-10,
    "company_interest": 0-10,
    "impact": 0-10,
    "logistics": 0-10
  },
  "reason": "2-3 sentence explanation",
  "highlights": ["max 3 positives"],
  "flags": ["max 3 concerns"]
}

"""

# ── Seen tracking ─────────────────────────────────────────────────────────────

def load_set(path: Path) -> set:
    if path.exists():
        return set(json.loads(path.read_text()))
    return set()

def save_set(path: Path, data: set):
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(sorted(data)))

def job_id(job: dict) -> str:
    raw = job.get("url") or f"{job.get('title','')}|{job.get('company','')}"
    return hashlib.md5(raw.encode()).hexdigest()

# ── ATS normalization ─────────────────────────────────────────────────────────

def safe_normalize(raw: dict, source: str) -> dict:
    return {
        "title": raw.get("title") or "Unknown Title",
        "company": raw.get("company") or source,
        "location": raw.get("location") or "Unknown",
        "description": (raw.get("description") or "")[:3000],
        "url": raw.get("url") or "",
        "source": source,
        "salary_min": raw.get("salary_min"),
        "salary_max": raw.get("salary_max"),
    }

# ── Stage 1: Ingestion ────────────────────────────────────────────────────────

def fetch_adzuna() -> list[dict]:
    log.info("Fetching Adzuna...")
    jobs = []
    queries = [
        "data analyst SQL", "product analyst", "product analytics",
        "analytics engineer", "data scientist product", "growth analyst",
        "business analyst SQL", "business operations analyst", "bizops analyst",
        "strategy analyst", "operations analyst", "revenue operations analyst",
        "product operations", "product manager analytics", "growth product manager",
        "experiment analyst", "A/B testing analyst", "customer analytics",
        "marketing analytics",
    ]
    base = "https://api.adzuna.com/v1/api/jobs/ca/search/1"
    for q in queries:
        try:
            r = requests.get(base, params={
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "what": q,
                "where": "Toronto",
                "results_per_page": 20,
            }, timeout=15)
            r.raise_for_status()
            for item in r.json().get("results", []):
                jobs.append(safe_normalize({
                    "title": item.get("title", ""),
                    "company": item.get("company", {}).get("display_name", ""),
                    "location": item.get("location", {}).get("display_name", ""),
                    "description": item.get("description", ""),
                    "url": item.get("redirect_url", ""),
                    "salary_min": item.get("salary_min"),
                    "salary_max": item.get("salary_max"),
                }, "adzuna"))
        except Exception as e:
            log.warning(f"Adzuna query '{q}' failed: {e}")
    return jobs


def fetch_greenhouse(slug: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            timeout=10,
        )
        if r.status_code in (404, 410):
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json().get("jobs", []):
            jobs.append(safe_normalize({
                "title": item.get("title", ""),
                "company": slug,
                "location": " | ".join(o.get("name", "") for o in item.get("offices", [])),
                "description": item.get("content", ""),
                "url": item.get("absolute_url", ""),
            }, f"greenhouse/{slug}"))
        return jobs
    except Exception:
        return []


def fetch_lever(slug: str) -> list[dict]:
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            timeout=10,
        )
        if r.status_code == 404:
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json():
            jobs.append(safe_normalize({
                "title": item.get("text", ""),
                "company": slug,
                "location": item.get("categories", {}).get("location", ""),
                "description": item.get("descriptionPlain", ""),
                "url": item.get("hostedUrl", ""),
            }, f"lever/{slug}"))
        return jobs
    except Exception:
        return []


def fetch_workable(slug: str) -> list[dict]:
    try:
        r = requests.post(
            f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
            json={"query": "", "limit": 100},
            timeout=10,
        )
        if r.status_code in (404, 422):
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json().get("results", []):
            jobs.append(safe_normalize({
                "title": item.get("title", ""),
                "company": slug,
                "location": item.get("city", "") or "Remote",
                "description": item.get("description", ""),
                "url": f"https://apply.workable.com/{slug}/j/{item.get('shortcode','')}",
            }, f"workable/{slug}"))
        return jobs
    except Exception:
        return []


def fetch_ashby(slug: str) -> list[dict]:
    try:
        r = requests.post(
            "https://api.ashbyhq.com/posting-api/job-board",
            json={"organizationHostedJobsPageName": slug},
            timeout=10,
        )
        if r.status_code in (404, 422):
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json().get("jobs", []):
            location = item.get("location", "") or ""
            if item.get("isRemote"):
                location = f"Remote ({location})" if location else "Remote"
            jobs.append(safe_normalize({
                "title": item.get("title", ""),
                "company": slug,
                "location": location,
                "description": item.get("descriptionPlain", "") or item.get("descriptionHtml", ""),
                "url": item.get("jobUrl", ""),
            }, f"ashby/{slug}"))
        return jobs
    except Exception:
        return []


def fetch_ats_batch(label: str, slugs: list[str], fetch_fn) -> list[dict]:
    all_jobs = []
    for slug in slugs:
        jobs = fetch_fn(slug)
        if jobs:
            log.info(f"{label}/{slug}: {len(jobs)} raw jobs")
        all_jobs.extend(jobs)
    return all_jobs


# ── MaRS Getro job board ──────────────────────────────────────────────────────

def fetch_mars_jobs() -> list[dict]:
    log.info("Fetching MaRS jobs (Getro)...")
    jobs = []
    base_url = "https://techjobs.marsdd.com"
    page = 1
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://techjobs.marsdd.com/jobs",
    }

    while True:
        try:
            r = requests.get(
                f"{base_url}/api/v1/jobs",
                params={"page": page, "per_page": 100},
                headers=headers,
                timeout=15,
            )
            if r.status_code == 404:
                log.warning(
                    "MaRS Getro endpoint returned 404. "
                    "Check the live XHR calls at techjobs.marsdd.com/jobs "
                    "to confirm the current API path."
                )
                break
            if r.status_code != 200:
                log.warning(f"MaRS Getro returned {r.status_code} on page {page}")
                break

            data = r.json()
            items = data.get("jobs") or data.get("results") or []
            if not items:
                break

            for item in items:
                company_info = item.get("company") or item.get("startup") or {}
                company_name = (
                    company_info.get("name")
                    if isinstance(company_info, dict)
                    else str(company_info)
                )
                jobs.append(safe_normalize({
                    "title": item.get("title", ""),
                    "company": company_name or "Unknown",
                    "location": item.get("location") or item.get("city") or "Toronto",
                    "description": item.get("description") or item.get("body") or "",
                    "url": item.get("url") or item.get("job_url") or item.get("apply_url") or "",
                }, "mars_getro"))

            meta = data.get("meta") or data.get("pagination") or {}
            total_pages = meta.get("total_pages") or meta.get("pages") or 1
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

        except Exception as e:
            log.warning(f"MaRS Getro page {page} failed: {e}")
            break

    log.info(f"MaRS: {len(jobs)} raw jobs")
    return jobs


# ── Wellfound SEO pages ───────────────────────────────────────────────────────

def fetch_wellfound_seo() -> list[dict]:
    log.info("Fetching Wellfound SEO pages...")
    jobs = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for role_slug in WELLFOUND_ROLES:
        url = f"https://wellfound.com/role/l/{role_slug}/toronto"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                log.warning(f"Wellfound SEO {role_slug}: HTTP {r.status_code}")
                continue

            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                r.text,
                re.DOTALL,
            )
            if not match:
                log.warning(f"Wellfound SEO {role_slug}: no __NEXT_DATA__ found")
                continue

            data = json.loads(match.group(1))
            apollo = (
                data.get("props", {})
                    .get("pageProps", {})
                    .get("apolloState", {})
            )

            found_on_page = 0
            for key, val in apollo.items():
                if not isinstance(val, dict):
                    continue
                if not (key.startswith("JobListing:") or key.startswith("StartupRole:")):
                    continue

                title = val.get("title") or val.get("text") or ""
                if not title:
                    continue

                startup = val.get("startup") or {}
                if isinstance(startup, dict):
                    company_name = startup.get("name", "")
                else:
                    ref = startup.get("__ref", "")
                    startup_obj = apollo.get(ref, {})
                    company_name = startup_obj.get("name", "") if isinstance(startup_obj, dict) else ""

                job_url = val.get("url") or val.get("applyUrl") or ""
                if val.get("slug") and not job_url:
                    job_url = f"https://wellfound.com/jobs/{val['slug']}"

                jobs.append(safe_normalize({
                    "title": title,
                    "company": company_name,
                    "location": "Toronto",
                    "description": val.get("description") or val.get("descriptionSnippet") or "",
                    "url": job_url,
                }, "wellfound_seo"))
                found_on_page += 1

            log.info(f"Wellfound SEO {role_slug}: {found_on_page} jobs")
            time.sleep(1.0)

        except Exception as e:
            log.warning(f"Wellfound SEO {role_slug} failed: {e}")

    log.info(f"Wellfound SEO total: {len(jobs)} raw jobs")
    return jobs


# ── Remotive free API ─────────────────────────────────────────────────────────

def fetch_remotive() -> list[dict]:
    log.info("Fetching Remotive...")
    jobs = []

    for search_term in REMOTIVE_SEARCHES:
        try:
            r = requests.get(
                "https://remotive.com/api/remote-jobs",
                params={"search": search_term, "limit": 50},
                timeout=15,
            )
            r.raise_for_status()
            for item in r.json().get("jobs", []):
                jobs.append(safe_normalize({
                    "title": item.get("title", ""),
                    "company": item.get("company_name", ""),
                    "location": item.get("candidate_required_location") or "Remote",
                    "description": item.get("description", ""),
                    "url": item.get("url", ""),
                    "salary_min": None,
                    "salary_max": None,
                }, "remotive"))
        except Exception as e:
            log.warning(f"Remotive '{search_term}' failed: {e}")
        time.sleep(0.3)

    log.info(f"Remotive: {len(jobs)} raw jobs")
    return jobs


# ── YC DISCOVERY (via yc-oss/api static mirror) ───────────────────────────────

# Base URL for the community-maintained daily mirror of YC's Algolia index.
# Unlike Algolia's public key (which embeds a validUntil and rotates), this
# mirror is static JSON on raw.githubusercontent.com and never requires auth.
_YC_OSS_BASE = "https://raw.githubusercontent.com/yc-oss/api/main/companies"

# Map each of our relevant tags to the per-tag JSON file in the mirror.
# Files live at <base>/<tag-slug>.json (spaces → hyphens, lowercase).
# Tags not present in the repo fall back to the full hiring.json filter.
_YC_TAG_SLUGS = {
    "gaming":           "gaming",
    "consumer":         "consumer",
    "education":        "education",
    "edtech":           "edtech",
    "health":           "health",
    "healthcare":       "healthcare",
    "entertainment":    "entertainment",
    "media":            "media",
    "fitness":          "fitness",
    "mental health":    "mental-health",
    "marketplace":      "marketplace",
    "productivity":     "productivity",
    "community":        "community",
    "sports":           "sports",
    "creator economy":  "creator-economy",
    "social":           "social",
    "food":             "food",
    "food and beverage":"food-and-beverage",
    "travel":           "travel",
    "music":            "music",
    "art":              "art",
    "climate":          "climate",
    "sustainability":   "sustainability",
    "developer tools":  "developer-tools",
    "retail":           "retail",
    "e-commerce":       "e-commerce",
}


def fetch_yc_companies() -> list[dict]:
    """
    Fetch YC hiring companies from the yc-oss/api community mirror.

    Strategy:
    1. Try each per-tag JSON file (these are small and pre-filtered).
    2. Fall back to the full hiring.json and filter client-side if a tag
       file 404s (the repo may not have every tag as a separate file yet).

    Returns a deduplicated list of dicts with keys: name, slug, tags.
    """
    log.info("Fetching YC companies via yc-oss/api mirror...")

    seen_slugs: set[str] = set()
    companies: list[dict] = []

    headers = {"Accept": "application/json"}

    for tag, tag_slug in _YC_TAG_SLUGS.items():
        url = f"{_YC_OSS_BASE}/{tag_slug}.json"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 404:
                log.debug(f"YC tag file not found: {tag_slug}.json — will rely on hiring.json fallback")
                continue
            r.raise_for_status()
            for co in r.json():
                if not co.get("isHiring", True):  # mirror may include non-hiring; skip them
                    continue
                slug = co.get("slug") or re.sub(r"[^a-z0-9]+", "-", co.get("name", "").lower())
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                companies.append({
                    "name": co.get("name", ""),
                    "slug": slug,
                    "tags": co.get("tags") or [tag],
                })
        except Exception as e:
            log.warning(f"YC tag '{tag_slug}' fetch failed: {e}")

    # Fallback: if we got nothing from tag files, load the full hiring list
    # and filter client-side by YC_RELEVANT_TAGS.
    if not companies:
        log.info("YC tag files returned nothing — falling back to hiring.json")
        try:
            r = requests.get(f"{_YC_OSS_BASE}/hiring.json", headers=headers, timeout=20)
            r.raise_for_status()
            for co in r.json():
                co_tags = set(t.lower() for t in (co.get("tags") or []))
                if not co_tags & YC_RELEVANT_TAGS:
                    continue
                slug = co.get("slug") or re.sub(r"[^a-z0-9]+", "-", co.get("name", "").lower())
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                companies.append({
                    "name": co.get("name", ""),
                    "slug": slug,
                    "tags": list(co_tags),
                })
        except Exception as e:
            log.warning(f"YC hiring.json fallback failed: {e}")

    log.info(f"YC mirror: {len(companies)} relevant hiring companies")
    return companies


def probe_company_all_ats(name, slug):
    clean = re.sub(r"[^a-z0-9]+", "-", name.lower())
    variants = [slug, clean]

    found = []
    for fn in [fetch_greenhouse, fetch_lever, fetch_workable, fetch_ashby]:
        for v in variants:
            jobs = fn(v)
            if jobs:
                found.extend(jobs)
                break
    return found


def fetch_yc_discovered(seen_yc):
    companies = fetch_yc_companies()
    new = [c for c in companies if c["slug"] not in seen_yc]

    all_jobs = []
    new_slugs = set()

    for c in new:
        jobs = probe_company_all_ats(c["name"], c["slug"])
        all_jobs.extend(jobs)
        new_slugs.add(c["slug"])

    return all_jobs, seen_yc | new_slugs


# ── OBJECTIVE FILTERING ───────────────────────────────────────────────────────

def is_valid_job(job: dict) -> tuple[bool, str | None]:
    title = (job.get("title") or "").lower()
    desc  = (job.get("description") or "").lower()
    text  = f"{title} {desc}"
    location = (job.get("location") or "").lower()

    if not job.get("title") or not job.get("url"):
        return False, "missing_title_or_url"

    if "engineer" in title:
        return False, "engineer_title"

    if not ("sql" in text):
        return False, "no_sql"

    if job.get("source") == "adzuna":
        if not (
            "canada" in location or
            "toronto" in location or
            "remote" in location
        ):
            return False, "location_filtered"

    return True, None


# ── CLAUDE EVAL ──────────────────────────────────────────────────────────────

def evaluate_job(client, job):
    log.info(f"Evaluating {job['title']} @ {job['company']}")

    prompt = f"""
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Description:
{job['description'][:2000]}
"""

    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        system=MATCHING_CRITERIA,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    text = resp.content[0].text.strip()

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON found in Claude output: {text[:200]}")

    result = json.loads(match.group(0))
    bd = result.get("breakdown", {})
    overall = result.get("score")

    log.info(
        f"\n"
        f"JOB: {job['title']} @ {job['company']}\n"
        f"URL: {job.get('url')}\n"
        f"Score: {overall}\n"
        f"  career={bd.get('career_work_quality')}\n"
        f"  company={bd.get('company_interest')}\n"
        f"  impact={bd.get('impact')}\n"
        f"  logistics={bd.get('logistics')}\n"
    )

    return result


# ── EMAIL DIGEST ──────────────────────────────────────────────────────────────

def send_digest(matches: list[dict]):
    if not matches:
        log.info("No matches above threshold — skipping email.")
        return

    date_str = datetime.today().strftime("%b %d, %Y")

    html_rows = ""
    for m in matches:
        j = m["job"]
        r = m["result"]
        bd = r.get("breakdown", {})
        highlights = "<br>".join(f"✓ {h}" for h in r.get("highlights", []))
        flags = "<br>".join(f"⚠ {f}" for f in r.get("flags", []))
        html_rows += f"""
        <tr>
          <td style="padding:10px;vertical-align:top">
            <b>{j['title']}</b><br>
            <span style="color:#555">{j['company']}</span><br>
            <small style="color:#888">{j['location']} &nbsp;·&nbsp; {j['source']}</small>
          </td>
          <td style="padding:10px;text-align:center;vertical-align:top">
            <span style="font-size:22px;font-weight:bold;color:#1a73e8">{r.get('score')}</span><br>
            <small style="color:#888">/10</small>
          </td>
          <td style="padding:10px;vertical-align:top;font-size:12px;color:#444">
            Career: {bd.get('career_work_quality')}&nbsp;
            Co: {bd.get('company_interest')}&nbsp;
            Impact: {bd.get('impact')}&nbsp;
            Logistics: {bd.get('logistics')}
          </td>
          <td style="padding:10px;vertical-align:top;font-size:12px">
            {r.get('reason', '')}
          </td>
          <td style="padding:10px;vertical-align:top;font-size:12px">
            {highlights}
          </td>
          <td style="padding:10px;vertical-align:top;font-size:12px;color:#b00">
            {flags}
          </td>
          <td style="padding:10px;vertical-align:top">
            <a href="{j['url']}" style="background:#1a73e8;color:#fff;padding:6px 12px;border-radius:4px;text-decoration:none;font-size:12px">Apply</a>
          </td>
        </tr>"""

    html = f"""
    <html>
    <body style="font-family:sans-serif;font-size:13px;color:#222;background:#f9f9f9;padding:20px">
      <h2 style="color:#1a73e8">Job Scout — {date_str}</h2>
      <p style="color:#555">{len(matches)} role{"s" if len(matches) != 1 else ""} scored {SCORE_THRESHOLD}+ today</p>
      <table border="0" cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1)">
        <tr style="background:#f0f4ff;font-size:11px;text-transform:uppercase;color:#888;font-weight:bold">
          <th style="padding:10px;text-align:left">Role</th>
          <th style="padding:10px">Score</th>
          <th style="padding:10px;text-align:left">Breakdown</th>
          <th style="padding:10px;text-align:left">Reason</th>
          <th style="padding:10px;text-align:left">Highlights</th>
          <th style="padding:10px;text-align:left">Flags</th>
          <th style="padding:10px;text-align:left">Link</th>
        </tr>
        {html_rows}
      </table>
      <p style="color:#aaa;font-size:11px;margin-top:20px">Job Scout · {date_str}</p>
    </body>
    </html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Job Scout: {len(matches)} match{'es' if len(matches) != 1 else ''} — {date_str}"
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = GMAIL_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            smtp.sendmail(GMAIL_SENDER, GMAIL_RECIPIENT, msg.as_string())
        log.info(f"Digest sent to {GMAIL_RECIPIENT}: {len(matches)} matches.")
    except Exception as e:
        log.error(f"Failed to send digest email: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Job Scout...")

    seen_jobs = load_set(SEEN_JOBS_FILE)
    seen_yc = load_set(SEEN_YC_SLUGS_FILE)

    jobs = []
    jobs += fetch_adzuna()
    jobs += fetch_ats_batch("greenhouse", GREENHOUSE_COMPANIES, fetch_greenhouse)
    jobs += fetch_ats_batch("lever", LEVER_COMPANIES, fetch_lever)
    jobs += fetch_ats_batch("workable", WORKABLE_COMPANIES, fetch_workable)
    jobs += fetch_ats_batch("ashby", ASHBY_COMPANIES, fetch_ashby)
    jobs += fetch_mars_jobs()
    jobs += fetch_wellfound_seo()
    jobs += fetch_remotive()

    yc_jobs, seen_yc = fetch_yc_discovered(seen_yc)
    jobs += yc_jobs

    # Deduplicate against seen jobs
    new_jobs = []
    for j in jobs:
        jid = job_id(j)
        if jid not in seen_jobs:
            new_jobs.append(j)
            seen_jobs.add(jid)

    log.info(f"Total raw: {len(jobs)} | New (unseen): {len(new_jobs)}")

    # Objective filter
    filtered = []
    filter_counts: dict[str, int] = {}
    for j in new_jobs:
        ok, reason = is_valid_job(j)
        if ok:
            filtered.append(j)
        else:
            filter_counts[reason] = filter_counts.get(reason, 0) + 1

    log.info(f"Post-filter: {len(filtered)} jobs | Dropped: {filter_counts}")

    # Claude evaluation
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    strong_matches = []

    for j in filtered[:20]:
        try:
            result = evaluate_job(client, j)
            if result.get("score", 0) >= SCORE_THRESHOLD:
                strong_matches.append({"job": j, "result": result})
        except Exception as e:
            log.warning(f"Evaluation failed for {j.get('title')}: {e}")

    log.info(f"Strong matches (score >= {SCORE_THRESHOLD}): {len(strong_matches)}")

    # Send digest
    send_digest(strong_matches)

    # Persist seen state
    save_set(SEEN_JOBS_FILE, seen_jobs)
    save_set(SEEN_YC_SLUGS_FILE, seen_yc)

    log.info("Job Scout complete.")


if __name__ == "__main__":
    main()
