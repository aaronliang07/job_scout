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

SEEN_JOBS_FILE = Path("data/seen_jobs.json")

# ── Claude Matching Criteria ──────────────────────────────────────────────────

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
    if not j["title"]:    j["title"] = "Unknown Title"
    if not j["location"]: j["location"] = "Unknown"
    if not j["description"]: j["description"] = ""
    return j

# ── Stage 1: Ingestion ────────────────────────────────────────────────────────

def fetch_adzuna():
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
            log.warning(f"  Adzuna query '{q}' failed: {e}")
    log.info(f"  Adzuna: {len(jobs)} raw jobs")
    return jobs


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

# ── Stage 2: Objective filtering ──────────────────────────────────────────────

def is_valid_job(job: dict) -> tuple[bool, str | None]:
    """Returns (is_valid, rejection_reason). rejection_reason is None if valid."""
    title = (job.get("title") or "").lower()
    desc  = (job.get("description") or "").lower()
    text  = f"{title} {desc}"

    if not job.get("title") or not job.get("url"):
        return False, "missing_title_or_url"

    if "engineer" in title:
        return False, "engineer_title"

    if not ("sql" in text or "python" in text):
        return False, "no_sql_or_python"

    return True, None

# ── Stage 3: Claude evaluation ────────────────────────────────────────────────

def evaluate_job(client, job: dict):
    log.info(f"  Evaluating: {job['title']} @ {job['company']}")

    t0 = time.time()
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
        elapsed = time.time() - t0
        result = json.loads(resp.content[0].text.strip())

        bd = result.get("breakdown", {})
        recommend = result.get("recommend", False)
        score = result.get("score", "?")
        flag = "✅ MATCH" if recommend else "✗  skip"

        log.info(
            f"    {flag} | score: {score}/10 | "
            f"career: {bd.get('career_work_quality','?')} "
            f"company: {bd.get('company_interest','?')} "
            f"impact: {bd.get('impact','?')} "
            f"logistics: {bd.get('logistics','?')} "
            f"| {elapsed:.1f}s"
        )
        if not recommend:
            log.info(f"    reason: {result.get('reason','')[:120]}")

        return result

    except Exception as e:
        elapsed = time.time() - t0
        log.warning(f"    Claude eval failed ({elapsed:.1f}s): {e}")
        return None

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    run_start = time.time()
    log.info("=== Job Scout starting ===")

    seen = load_set(SEEN_JOBS_FILE)
    log.info(f"Previously seen: {len(seen)} jobs")

    # ── Ingestion ──
    log.info("--- Stage 1: Ingestion ---")
    t0 = time.time()

    source_jobs: dict[str, list] = {}

    adzuna = fetch_adzuna()
    source_jobs["adzuna"] = adzuna

    greenhouse_slugs = ["riotgames", "figma", "shopify"]
    for slug in greenhouse_slugs:
        jobs = fetch_greenhouse(slug)
        source_jobs[f"greenhouse/{slug}"] = jobs
        log.info(f"  greenhouse/{slug}: {len(jobs)} raw jobs")

    lever_slugs = ["discord", "substack"]
    for slug in lever_slugs:
        jobs = fetch_lever(slug)
        source_jobs[f"lever/{slug}"] = jobs
        log.info(f"  lever/{slug}: {len(jobs)} raw jobs")

    workable_slugs = ["koho", "wattpad"]
    for slug in workable_slugs:
        jobs = fetch_workable(slug)
        source_jobs[f"workable/{slug}"] = jobs
        log.info(f"  workable/{slug}: {len(jobs)} raw jobs")

    all_jobs = [job for jobs in source_jobs.values() for job in jobs]
    log.info(f"Ingestion complete: {len(all_jobs)} total raw jobs ({time.time()-t0:.1f}s)")

    # ── Deduplication + filtering ──
    log.info("--- Stage 2: Dedup + Objective filtering ---")
    filter_counts = {"already_seen": 0, "engineer_title": 0, "no_sql_or_python": 0, "missing_title_or_url": 0}
    new_jobs = []

    for job in all_jobs:
        jid = job_id(job)
        if jid in seen:
            filter_counts["already_seen"] += 1
            continue
        valid, reason = is_valid_job(job)
        if not valid:
            filter_counts[reason] += 1
            continue
        new_jobs.append(job)

    log.info(f"  Already seen:        {filter_counts['already_seen']}")
    log.info(f"  Engineer title:      {filter_counts['engineer_title']}")
    log.info(f"  No SQL or Python:    {filter_counts['no_sql_or_python']}")
    log.info(f"  Missing title/URL:   {filter_counts['missing_title_or_url']}")
    log.info(f"  Passed filters:      {len(new_jobs)}")

    if not new_jobs:
        log.info("No new jobs to evaluate.")
        save_set(SEEN_JOBS_FILE, seen)
        log.info(f"=== Done in {time.time()-run_start:.1f}s ===")
        return

    # ── Claude evaluation ──
    log.info("--- Stage 3: Claude evaluation ---")
    t0 = time.time()
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    matches = []

    for job in new_jobs:
        result = evaluate_job(client, job)
        seen.add(job_id(job))
        if result and result.get("recommend"):
            matches.append((job, result))

    eval_time = time.time() - t0
    log.info(f"Evaluation complete: {len(new_jobs)} jobs in {eval_time:.1f}s ({eval_time/len(new_jobs):.1f}s avg)")

    save_set(SEEN_JOBS_FILE, seen)

    # ── Final summary ──
    total_time = time.time() - run_start
    log.info("--- Run summary ---")
    log.info(f"  Total ingested:      {len(all_jobs)}")
    log.info(f"  Already seen:        {filter_counts['already_seen']}")
    log.info(f"  Filtered out:        {filter_counts['engineer_title'] + filter_counts['no_sql_or_python'] + filter_counts['missing_title_or_url']}")
    log.info(f"  Evaluated by Claude: {len(new_jobs)}")
    log.info(f"  Matches:             {len(matches)}")
    log.info(f"  Total time:          {total_time:.1f}s")
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
