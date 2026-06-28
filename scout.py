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

SEEN_JOBS_FILE = Path("data/seen_jobs.json")

# ── Claude Matching Criteria (FULL RUBRIC RESTORED) ──────────────────────────

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

# ── ATS NORMALIZATION LAYER ───────────────────────────────────────────────────

def normalize_job(job: dict, source: str) -> dict:
    return {
        "title": job.get("title") or "",
        "company": job.get("company") or "",
        "location": job.get("location") or "",
        "description": job.get("description") or "",
        "url": job.get("url") or "",
        "source": source,
    }

def safe_normalize(job: dict, source: str) -> dict:
    j = normalize_job(job, source)

    if not j["title"]:
        j["title"] = "Unknown Title"
    if not j["location"]:
        j["location"] = "Unknown"
    if not j["description"]:
        j["description"] = ""

    return j

# ── Stage 1: INGESTION (NO FILTERING) ─────────────────────────────────────────

def fetch_adzuna():
    log.info("Fetching Adzuna...")
    jobs = []

    queries = [
        "data analyst SQL",
        "product analyst",
        "product analytics",
        "analytics engineer",
        "data scientist product",
        "growth analyst",
        "business analyst SQL",
        "business operations analyst",
        "bizops analyst",
        "strategy analyst",
        "operations analyst",
        "revenue operations analyst",
        "product operations",
        "product manager analytics",
        "growth product manager",
        "experiment analyst",
        "A/B testing analyst",
        "customer analytics",
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
            log.warning(f"Adzuna failed for {q}: {e}")

    return jobs

# ── ATS SOURCES ───────────────────────────────────────────────────────────────

def fetch_greenhouse(slug: str):
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code in (404, 410):
            return []

        jobs = []
        for item in r.json().get("jobs", []):
            jobs.append(safe_normalize({
                "title": item.get("title", ""),
                "company": slug,
                "location": " | ".join([o.get("name", "") for o in item.get("offices", [])]),
                "description": item.get("content", "")[:3000],
                "url": item.get("absolute_url", ""),
            }, f"greenhouse/{slug}"))

        return jobs

    except Exception:
        return []


def fetch_lever(slug: str):
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return []

        jobs = []
        for item in r.json():
            jobs.append(safe_normalize({
                "title": item.get("text", ""),
                "company": slug,
                "location": item.get("categories", {}).get("location", ""),
                "description": item.get("descriptionPlain", "")[:3000],
                "url": item.get("hostedUrl", ""),
            }, f"lever/{slug}"))

        return jobs

    except Exception:
        return []


def fetch_workable(slug: str):
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    try:
        r = requests.post(url, json={"query": "", "limit": 100}, timeout=10)
        if r.status_code in (404, 422):
            return []

        jobs = []
        for item in r.json().get("results", []):
            jobs.append(safe_normalize({
                "title": item.get("title", ""),
                "company": slug,
                "location": item.get("city", "") or "Remote",
                "description": item.get("description", "")[:3000],
                "url": f"https://apply.workable.com/{slug}/j/{item.get('shortcode','')}",
            }, f"workable/{slug}"))

        return jobs

    except Exception:
        return []

# ── Stage 2: OBJECTIVE FILTERING ──────────────────────────────────────────────

def is_valid_job(job: dict) -> bool:
    title = (job.get("title") or "").lower()
    desc  = (job.get("description") or "").lower()
    text  = f"{title} {desc}"

    # Filter 1: no engineer titles
    if "engineer" in title:
        return False

    # Filter 2: must contain SQL or Python
    if not ("sql" in text or "python" in text):
        return False

    # basic validity
    if not job.get("title") or not job.get("url"):
        return False

    return True

# ── Stage 3: CLAUDE EVALUATION ───────────────────────────────────────────────

def evaluate_job(client, job: dict):
    prompt = f"""
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Source: {job['source']}
Salary: {job.get('salary_min')} – {job.get('salary_max')}
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

        return json.loads(resp.content[0].text.strip())

    except Exception as e:
        log.warning(f"Claude eval failed: {e}")
        return None

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Job Scout starting ===")

    seen = load_set(SEEN_JOBS_FILE)

    all_jobs = []
    all_jobs += fetch_adzuna()

    for slug in ["riotgames", "figma", "shopify"]:
        all_jobs += fetch_greenhouse(slug)

    for slug in ["discord", "substack"]:
        all_jobs += fetch_lever(slug)

    for slug in ["koho", "wattpad"]:
        all_jobs += fetch_workable(slug)

    log.info(f"Ingested: {len(all_jobs)} jobs")

    new_jobs = []
    for job in all_jobs:
        jid = job_id(job)

        if jid in seen:
            continue

        if not is_valid_job(job):
            continue

        new_jobs.append(job)

    log.info(f"After filtering: {len(new_jobs)} jobs")

    if not new_jobs:
        log.info("No jobs to evaluate")
        return

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    matches = []

    for job in new_jobs:
        result = evaluate_job(client, job)
        seen.add(job_id(job))

        if result and result.get("recommend"):
            matches.append((job, result))

    save_set(SEEN_JOBS_FILE, seen)

    log.info(f"Matches: {len(matches)}")
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
