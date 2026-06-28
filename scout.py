"""
Job Scout — Daily job search engine for Aaron

Architecture:
1. Ingestion (raw ATS + Adzuna, no filtering)
2. ATS normalization layer (canonical schema)
3. Objective filtering (SQL/Python + non-engineer rule)
4. Claude scoring (structured rubric)
5. Optional email digest
"""

import os
import json
import hashlib
import logging
import time
import re
import requests
from datetime import datetime
from pathlib import Path
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

ADZUNA_APP_ID     = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY    = os.environ["ADZUNA_APP_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

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

Return ONLY JSON:
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


# ── YC DISCOVERY (UNCHANGED ORIGINAL RESTORED) ───────────────────────────────

def fetch_yc_companies():
    log.info("Fetching YC company directory...")
    try:
        r = requests.get("https://www.ycombinator.com/companies")
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text, re.DOTALL)
        if match:
            data = json.loads(match.group(1))
            companies = data["props"]["pageProps"]["companies"]
            return _filter_yc_companies(companies)
    except Exception as e:
        log.warning(f"YC fetch failed: {e}")

    return _fetch_yc_algolia_fallback()


def _filter_yc_companies(raw):
    results = []
    for co in raw:
        tags = set([t.lower() for t in (co.get("tags") or [])])
        if not tags.intersection(YC_RELEVANT_TAGS):
            continue
        if co.get("is_hiring") is False:
            continue
        name = co.get("name", "")
        slug = co.get("slug") or re.sub(r"[^a-z0-9]+", "-", name.lower())
        results.append({"name": name, "slug": slug, "tags": list(tags)})
    return results


def _fetch_yc_algolia_fallback():
    companies = []
    try:
        r = requests.post(
            "https://45bwzj1sgc-dsn.algolia.net/1/indexes/YCCompany_production/query",
            headers={
                "X-Algolia-Application-Id": "45BWZJ1SGC",
                "X-Algolia-API-Key": "Zjk5ZmE5OGY4NjZlZWE4MGNiMWVhYzgyY2ZlOTdlOThhNGQ1NDMxMzE3ZmZkMzE=",
            },
            json={"query": "startup", "hitsPerPage": 50},
        )
        if r.status_code == 200:
            for hit in r.json().get("hits", []):
                companies.append({
                    "name": hit.get("name"),
                    "slug": hit.get("slug"),
                    "tags": hit.get("tags", []),
                })
    except Exception:
        pass
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


# ── OBJECTIVE FILTERING (ONLY CHANGE HERE) ───────────────────────────────────

def is_valid_job(job: dict) -> tuple[bool, str | None]:
    title = (job.get("title") or "").lower()
    desc  = (job.get("description") or "").lower()
    text  = f"{title} {desc}"
    location = (job.get("location") or "").lower()

    if not job.get("title") or not job.get("url"):
        return False, "missing_title_or_url"

    if "engineer" in title:
        return False, "engineer_title"

    if not ("sql" in text or "python" in text):
        return False, "no_sql_or_python"

    # NEW: location filter
    if not (
        "canada" in location or
        "toronto" in location or
        "remote" in location
    ):
        return False, "location_filtered"

    return True, None


# ── CLAUDE EVAL (UNCHANGED) ─────────────────────────────────────────────────

def evaluate_job(client, job):
    log.info(f"Evaluating {job['title']}")
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
    )
    return json.loads(resp.content[0].text)


# ── MAIN (UNCHANGED STRUCTURE) ──────────────────────────────────────────────

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

    yc_jobs, seen_yc = fetch_yc_discovered(seen_yc)
    jobs += yc_jobs

    filtered = []
    for j in jobs:
        ok, _ = is_valid_job(j)
        if ok:
            filtered.append(j)

    log.info(f"Filtered jobs: {len(filtered)}")

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    for j in filtered[:20]:
        evaluate_job(client, j)


if __name__ == "__main__":
    main()
