"""
Job Scout — Daily job search engine for Aaron
Sources: Adzuna API + Greenhouse/Lever/Workable (manual list + YC auto-discovery)
Matching: Claude API (qualitative + quantitative criteria)
Delivery: Gmail digest
"""

import os
import json
import hashlib
import logging
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
GMAIL_SENDER      = os.environ["GMAIL_SENDER"]
GMAIL_RECIPIENT   = os.environ.get("GMAIL_RECIPIENT", GMAIL_SENDER)

SEEN_JOBS_FILE        = Path("data/seen_jobs.json")
SEEN_YC_SLUGS_FILE    = Path("data/seen_yc_slugs.json")   # cache probed YC slugs

# ── Matching criteria ─────────────────────────────────────────────────────────

MATCHING_CRITERIA = """
You are evaluating job postings for Aaron, a Senior Product Data Analyst based in Toronto.

HARD CRITERIA (all must be true to recommend):
1. Role involves quantitative/analytical work — SQL must be mentioned OR the role is clearly data-heavy (analyst, data scientist, analytics engineer, etc.)
2. Seniority is mid-level or senior (not entry-level / junior / intern)
3. Location is one of: Toronto (in-person or hybrid), fully remote, or remote anywhere in Canada
4. Company appears to be a real product company — not a staffing agency, consulting firm, or recruiter posting on behalf of an unknown client

STRONG POSITIVE SIGNALS (not required, but boost score):
- Gaming, entertainment, sports, or media industry
- Consumer-facing product with a real human impact (health, education, community, creativity)
- Mention of product analytics, experimentation, A/B testing, or growth analytics
- Mention of Python, dbt, Looker, Amplitude, Databricks, or causal inference
- Company has a clear mission beyond "we do software"

NEGATIVE SIGNALS (reduce score or reject):
- Pure finance / fintech / insurance with no consumer angle
- Vague "data consultant" or "analytics consultant" roles at Big 4 firms
- Role is primarily reporting/dashboarding with no analytical depth
- Job description is very short and generic (likely a low-effort recruiter post)
- Salary range (if mentioned) is clearly below market for Toronto senior roles (below ~$90k CAD)

Respond ONLY with a JSON object, no markdown, no explanation outside it:
{
  "recommend": true or false,
  "score": 1-10,
  "reason": "2-3 sentence explanation of why or why not",
  "highlights": ["up to 3 specific positive things about this role"],
  "flags": ["any concerns or missing info"]
}
"""

# ── Manual company lists ──────────────────────────────────────────────────────

GREENHOUSE_COMPANIES = [
    # Gaming & Entertainment
    "riotgames", "epicgames", "rovio", "scopely", "niantic",
    "kabam", "digitalextremers",
    # Consumer tech / human-centered
    "duolingo", "headspace", "calm", "strava", "allbirds",
    "peloton", "bumble", "eventbrite", "etsy", "patreon",
    "khanacademy", "coursera", "notion", "figma",
    # Canadian / Toronto tech
    "shopify", "wealthsimple", "hootsuite", "lightspeed",
    "ritual", "points", "ecobee", "d2l",
    # Other interesting product companies
    "airtable", "miro", "loom", "linear", "vercel",
    "retool", "segment", "mixpanel", "amplitude",
]

LEVER_COMPANIES = [
    # Gaming
    "nexon", "naughtydog",
    # Consumer / human-centered
    "whoop", "oura", "noom", "hims", "glossier", "warbyparker",
    "faire",
    # Canadian
    "benchsci", "sonder", "absorb",
    # Other
    "discord", "substack", "beehiiv",
]

WORKABLE_COMPANIES = [
    # Canadian / Toronto
    "mynd", "koho", "tulip-retail", "nudge-rewards",
    "wattpad", "achievers", "ada-support",
    # Consumer / human-centered
    "hubstaff", "typeform", "remote",
    # Gaming / entertainment
    "miniclip", "rovio-entertainment",
]

# YC tags we care about — company must match at least one
YC_RELEVANT_TAGS = {
    "gaming", "consumer", "education", "edtech", "health", "healthcare",
    "entertainment", "media", "fitness", "mental health", "marketplace",
    "productivity", "developer tools", "community", "sports", "creator economy",
    "social", "food", "travel", "music", "art", "climate", "sustainability",
}

# ── Seen jobs / YC slug cache ─────────────────────────────────────────────────

def load_json_set(path: Path) -> set:
    if path.exists():
        return set(json.loads(path.read_text()))
    return set()

def save_json_set(path: Path, data: set):
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(sorted(data)))

def job_id(job: dict) -> str:
    raw = job.get("url") or f"{job.get('title','')}|{job.get('company','')}"
    return hashlib.md5(raw.encode()).hexdigest()

# ── Data role title filter ────────────────────────────────────────────────────

DATA_KEYWORDS = [
    "data", "analyst", "analytics", "scientist", "intelligence",
    "insight", "bi ", "business intelligence", "quantitative",
]

def is_data_role(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in DATA_KEYWORDS)

# ── Source: Adzuna ────────────────────────────────────────────────────────────

def fetch_adzuna(max_results: int = 50) -> list[dict]:
    log.info("Fetching from Adzuna...")
    jobs = []
    queries = [
        "data analyst SQL",
        "product analyst",
        "analytics engineer",
        "data scientist product",
    ]
    base = "https://api.adzuna.com/v1/api/jobs/ca/search/1"
    for q in queries:
        try:
            r = requests.get(base, params={
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "results_per_page": 20,
                "what": q,
                "where": "Toronto",
                "distance": 50,
                "max_days_old": 3,
                "content-type": "application/json",
            }, timeout=15)
            r.raise_for_status()
            for item in r.json().get("results", []):
                jobs.append({
                    "title":      item.get("title", ""),
                    "company":    item.get("company", {}).get("display_name", ""),
                    "location":   item.get("location", {}).get("display_name", ""),
                    "description":item.get("description", ""),
                    "url":        item.get("redirect_url", ""),
                    "source":     "adzuna",
                    "salary_min": item.get("salary_min"),
                    "salary_max": item.get("salary_max"),
                })
        except Exception as e:
            log.warning(f"Adzuna query '{q}' failed: {e}")
    log.info(f"Adzuna: {len(jobs)} raw listings")
    return jobs[:max_results]

# ── Source: Greenhouse ────────────────────────────────────────────────────────

def fetch_greenhouse(slug: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (404, 410):
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json().get("jobs", []):
            if not is_data_role(item.get("title", "")):
                continue
            location = " | ".join(
                loc.get("name", "") for loc in item.get("offices", [])
            ) or "Unknown"
            jobs.append({
                "title":       item.get("title", ""),
                "company":     slug.replace("-", " ").title(),
                "location":    location,
                "description": item.get("content", "")[:3000],
                "url":         item.get("absolute_url", ""),
                "source":      f"greenhouse/{slug}",
            })
        return jobs
    except Exception as e:
        log.debug(f"Greenhouse {slug}: {e}")
        return []

# ── Source: Lever ─────────────────────────────────────────────────────────────

def fetch_lever(slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json():
            if not is_data_role(item.get("text", "")):
                continue
            categories = item.get("categories", {})
            location   = categories.get("location", "Unknown")
            desc = item.get("descriptionPlain", "") or ""
            for extra in item.get("lists", []):
                desc += f"\n{extra.get('text','')}: " + ", ".join(
                    i.get("text", "") for i in extra.get("content", [])
                )
            jobs.append({
                "title":       item.get("text", ""),
                "company":     slug.replace("-", " ").title(),
                "location":    location,
                "description": desc[:3000],
                "url":         item.get("hostedUrl", ""),
                "source":      f"lever/{slug}",
            })
        return jobs
    except Exception as e:
        log.debug(f"Lever {slug}: {e}")
        return []

# ── Source: Workable ──────────────────────────────────────────────────────────

def fetch_workable(slug: str) -> list[dict]:
    """Fetch open roles from a Workable board."""
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    try:
        r = requests.post(url, json={"query": "", "limit": 100}, timeout=10)
        if r.status_code in (404, 422):
            return []
        r.raise_for_status()
        jobs = []
        for item in r.json().get("results", []):
            if not is_data_role(item.get("title", "")):
                continue
            location_parts = [
                item.get("city", ""),
                item.get("state", ""),
                item.get("country", ""),
            ]
            location = ", ".join(p for p in location_parts if p) or "Unknown"
            if item.get("remote"):
                location = f"Remote ({location})" if location != "Unknown" else "Remote"
            jobs.append({
                "title":       item.get("title", ""),
                "company":     item.get("account", {}).get("name", slug.replace("-", " ").title()),
                "location":    location,
                "description": item.get("description", "")[:3000],
                "url":         f"https://apply.workable.com/{slug}/j/{item.get('shortcode','')}",
                "source":      f"workable/{slug}",
            })
        return jobs
    except Exception as e:
        log.debug(f"Workable {slug}: {e}")
        return []

# ── Source: YC company auto-discovery ────────────────────────────────────────

def fetch_yc_companies() -> list[dict]:
    """
    Pull companies from the YC company directory API.
    Returns list of dicts with name, slug, tags, is_hiring.
    Only returns companies tagged with categories we care about.
    """
    log.info("Fetching YC company directory...")
    companies = []
    try:
        # YC exposes a public Algolia-backed search used by their directory
        r = requests.get(
            "https://www.ycombinator.com/companies",
            headers={"User-Agent": "Mozilla/5.0 (compatible; JobScout/1.0)"},
            timeout=15,
        )
        r.raise_for_status()
        # Extract the JSON blob that Next.js embeds in __NEXT_DATA__
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text, re.DOTALL)
        if not match:
            log.warning("YC: could not find __NEXT_DATA__ — site structure may have changed")
            return []
        data = json.loads(match.group(1))
        # Navigate to the companies list (path may vary with YC site updates)
        companies_raw = (
            data.get("props", {})
                .get("pageProps", {})
                .get("companies", [])
        )
        if not companies_raw:
            log.warning("YC: companies list empty — trying fallback API")
            return fetch_yc_api_fallback()

        for co in companies_raw:
            tags = {t.lower() for t in (co.get("tags") or co.get("industries") or [])}
            if not tags.intersection(YC_RELEVANT_TAGS):
                continue
            if not co.get("is_hiring", True):  # skip if explicitly not hiring
                continue
            name = co.get("name", "")
            slug = co.get("slug") or name.lower().replace(" ", "-")
            companies.append({"name": name, "slug": slug, "tags": list(tags)})

        log.info(f"YC: {len(companies)} relevant companies found")
    except Exception as e:
        log.warning(f"YC directory fetch failed: {e}")
        return fetch_yc_api_fallback()
    return companies


def fetch_yc_api_fallback() -> list[dict]:
    """
    Fallback: use YC's public Algolia search API to get companies by category.
    This is the same API their website uses under the hood.
    """
    log.info("YC fallback: trying Algolia API...")
    companies = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
        }
        # Search across categories we care about
        for tag in ["consumer", "gaming", "education", "health", "entertainment", "productivity"]:
            r = requests.post(
                "https://45bwzj1sgc-dsn.algolia.net/1/indexes/YCCompany_production/query",
                headers={
                    **headers,
                    "X-Algolia-Application-Id": "45BWZJ1SGC",
                    "X-Algolia-API-Key": "Zjk5ZmE5OGY4NjZlZWE4MGNiMWVhYzgyY2ZlOTdlOThhNGQ1NDMxMzE3ZmZkMzE=",
                },
                json={"query": tag, "hitsPerPage": 50, "filters": "is_hiring:true"},
                timeout=10,
            )
            if r.status_code != 200:
                continue
            for hit in r.json().get("hits", []):
                name = hit.get("name", "")
                slug = hit.get("slug") or name.lower().replace(" ", "-")
                if not any(c["slug"] == slug for c in companies):
                    companies.append({"name": name, "slug": slug, "tags": [tag]})
        log.info(f"YC fallback: {len(companies)} companies")
    except Exception as e:
        log.warning(f"YC fallback also failed: {e}")
    return companies


def probe_ats_for_slug(name: str, slug: str) -> list[dict]:
    """
    Given a company name and slug, probe all three ATS platforms and return
    any data roles found. Tries common slug variations.
    """
    # Generate slug variants to try
    clean = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    variants = list(dict.fromkeys([slug, clean]))  # deduplicate, preserve order

    found = []
    for v in variants:
        gh = fetch_greenhouse(v)
        if gh:
            log.info(f"  YC/{name}: Greenhouse match on '{v}' ({len(gh)} roles)")
            found.extend(gh)
            break  # found on Greenhouse, skip other variants

    for v in variants:
        lv = fetch_lever(v)
        if lv:
            log.info(f"  YC/{name}: Lever match on '{v}' ({len(lv)} roles)")
            found.extend(lv)
            break

    for v in variants:
        wk = fetch_workable(v)
        if wk:
            log.info(f"  YC/{name}: Workable match on '{v}' ({len(wk)} roles)")
            found.extend(wk)
            break

    return found


def fetch_yc_discovered(seen_slugs: set) -> tuple[list[dict], set]:
    """
    Auto-discover companies from YC directory and probe their ATS boards.
    Returns (new_jobs, updated_seen_slugs).
    Only probes companies we haven't tried before (cached in seen_yc_slugs.json).
    """
    companies = fetch_yc_companies()
    new_slugs  = set()
    all_jobs   = []

    # Only probe companies we haven't seen before
    new_companies = [c for c in companies if c["slug"] not in seen_slugs]
    log.info(f"YC: {len(new_companies)} new companies to probe (of {len(companies)} total)")

    for co in new_companies:
        jobs = probe_ats_for_slug(co["name"], co["slug"])
        all_jobs.extend(jobs)
        new_slugs.add(co["slug"])

    return all_jobs, seen_slugs | new_slugs

# ── Batch fetchers ────────────────────────────────────────────────────────────

def fetch_all_greenhouse() -> list[dict]:
    log.info(f"Greenhouse: checking {len(GREENHOUSE_COMPANIES)} manual companies...")
    jobs = []
    for slug in GREENHOUSE_COMPANIES:
        found = fetch_greenhouse(slug)
        if found:
            log.info(f"  {slug}: {len(found)} data roles")
        jobs.extend(found)
    return jobs

def fetch_all_lever() -> list[dict]:
    log.info(f"Lever: checking {len(LEVER_COMPANIES)} manual companies...")
    jobs = []
    for slug in LEVER_COMPANIES:
        found = fetch_lever(slug)
        if found:
            log.info(f"  {slug}: {len(found)} data roles")
        jobs.extend(found)
    return jobs

def fetch_all_workable() -> list[dict]:
    log.info(f"Workable: checking {len(WORKABLE_COMPANIES)} manual companies...")
    jobs = []
    for slug in WORKABLE_COMPANIES:
        found = fetch_workable(slug)
        if found:
            log.info(f"  {slug}: {len(found)} data roles")
        jobs.extend(found)
    return jobs

# ── Matching: Claude ──────────────────────────────────────────────────────────

def evaluate_job(client: Anthropic, job: dict) -> dict | None:
    prompt = f"""
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Source: {job['source']}
Salary: {job.get('salary_min', '?')} – {job.get('salary_max', '?')} CAD
Description:
{job['description'][:2500]}
"""
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system=MATCHING_CRITERIA,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        return json.loads(raw)
    except Exception as e:
        log.warning(f"Evaluation failed for {job['title']} @ {job['company']}: {e}")
        return None

# ── Email digest ──────────────────────────────────────────────────────────────

def format_email_body(matches: list[tuple[dict, dict]]) -> str:
    today = datetime.now().strftime("%B %d, %Y")
    lines = [
        f"<h2>Job Scout — {today}</h2>",
        f"<p>Found <strong>{len(matches)}</strong> new role(s) that match your criteria.</p>",
        "<hr>",
    ]
    for job, ev in matches:
        score      = ev.get("score", "?")
        highlights = ev.get("highlights", [])
        flags      = ev.get("flags", [])
        salary = ""
        if job.get("salary_min") and job.get("salary_max"):
            salary = f" · ${int(job['salary_min']):,}–${int(job['salary_max']):,} CAD"

        lines += [
            f"<h3><a href='{job['url']}'>{job['title']}</a></h3>",
            f"<p><strong>{job['company']}</strong> · {job['location']}{salary} · Score: {score}/10</p>",
            f"<p>{ev.get('reason','')}</p>",
        ]
        if highlights:
            lines.append("<ul>" + "".join(f"<li>✅ {h}</li>" for h in highlights) + "</ul>")
        if flags:
            lines.append("<ul>" + "".join(f"<li>⚠️ {f}</li>" for f in flags) + "</ul>")
        lines += [
            f"<p><a href='{job['url']}'>View posting →</a> · Source: {job['source']}</p>",
            "<hr>",
        ]
    lines.append("<p><em>Job Scout · runs daily via GitHub Actions</em></p>")
    return "\n".join(lines)

def send_email_via_gmail(subject: str, html_body: str):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    if not gmail_password:
        log.warning("GMAIL_APP_PASSWORD not set — printing to stdout")
        print(f"\n{'='*60}\nSUBJECT: {subject}\n{'='*60}\n{html_body}\n")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = GMAIL_RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_SENDER, gmail_password)
        server.sendmail(GMAIL_SENDER, GMAIL_RECIPIENT, msg.as_string())
    log.info(f"Email sent to {GMAIL_RECIPIENT}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Job Scout starting ===")

    seen_jobs = load_json_set(SEEN_JOBS_FILE)
    seen_yc   = load_json_set(SEEN_YC_SLUGS_FILE)
    log.info(f"Already seen: {len(seen_jobs)} jobs | {len(seen_yc)} YC companies probed")

    # 1. Gather all listings
    all_jobs: list[dict] = []
    all_jobs.extend(fetch_adzuna())
    all_jobs.extend(fetch_all_greenhouse())
    all_jobs.extend(fetch_all_lever())
    all_jobs.extend(fetch_all_workable())

    yc_jobs, seen_yc = fetch_yc_discovered(seen_yc)
    all_jobs.extend(yc_jobs)

    log.info(f"Total raw listings: {len(all_jobs)}")

    # 2. Deduplicate against seen jobs
    new_jobs = [j for j in all_jobs if job_id(j) not in seen_jobs]
    log.info(f"New (unseen) listings: {len(new_jobs)}")

    # Save updated YC slug cache regardless of whether there are new jobs
    save_json_set(SEEN_YC_SLUGS_FILE, seen_yc)

    if not new_jobs:
        log.info("No new jobs — nothing to do.")
        return

    # 3. Evaluate with Claude
    client  = Anthropic(api_key=ANTHROPIC_API_KEY)
    matches = []
    for job in new_jobs:
        log.info(f"Evaluating: {job['title']} @ {job['company']}")
        result = evaluate_job(client, job)
        if result and result.get("recommend"):
            log.info(f"  ✅ MATCH (score {result.get('score')}/10)")
            matches.append((job, result))
        else:
            reason = result.get("reason", "no result") if result else "eval failed"
            log.info(f"  ✗ skip — {reason[:80]}")
        seen_jobs.add(job_id(job))

    # 4. Persist seen jobs
    save_json_set(SEEN_JOBS_FILE, seen_jobs)
    log.info(f"Seen jobs updated: {len(seen_jobs)} total")

    # 5. Send digest
    if matches:
        matches.sort(key=lambda x: x[1].get("score", 0), reverse=True)
        subject = f"Job Scout: {len(matches)} new match{'es' if len(matches) > 1 else ''} — {datetime.now().strftime('%b %d')}"
        send_email_via_gmail(subject, format_email_body(matches))
    else:
        log.info("No matches today — no email sent.")

    log.info("=== Job Scout done ===")

if __name__ == "__main__":
    main()
