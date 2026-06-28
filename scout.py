"""
Job Scout — Daily job search engine for Aaron

Architecture:
1. Ingestion (raw ATS + Adzuna, no filtering)
2. ATS normalization layer (canonical schema)
3. Objective filtering (SQL/Python + non-engineer rule + location filter)
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

# ── DEAD ATS SLUGS (observed or inferred removed/migrated boards) ─────────────

DEAD_GREENHOUSE_SLUGS = {
    "niantic",
    "headspace",
    "bumble",
    "wealthsimple",
    "etsy",
    "patreon",
}

# ── Manual company lists ──────────────────────────────────────────────────────

GREENHOUSE_COMPANIES = [
    "riotgames", "epicgames", "scopely", "kabam",
    "duolingo", "calm", "strava", "peloton",
    "eventbrite", "khanacademy", "coursera", "figma",
    "hootsuite", "lightspeed", "d2l",
    "airtable", "mixpanel", "amplitude",
]

LEVER_COMPANIES = [
    "naughtydog", "whoop", "oura", "faire",
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
]

# ── Claude matching criteria ──────────────────────────────────────────────────

MATCHING_CRITERIA = """
You are evaluating job postings for Aaron, a Senior Product Data Analyst based in Toronto.

Return ONLY JSON with recommendation + score breakdown.
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
        "title":       raw.get("title") or "Unknown Title",
        "company":     raw.get("company") or source,
        "location":    raw.get("location") or "Unknown",
        "description": (raw.get("description") or "")[:3000],
        "url":         raw.get("url") or "",
        "source":      source,
        "salary_min":  raw.get("salary_min"),
        "salary_max":  raw.get("salary_max"),
    }

# ── LOCATION FILTER ──────────────────────────────────────────────────────────

def passes_location_filter(job: dict) -> bool:
    text = " ".join([
        job.get("location", ""),
        job.get("title", ""),
        job.get("description", "")
    ]).lower()

    return any(k in text for k in ["canada", "toronto", "remote"])

# ── Stage 1: Ingestion ────────────────────────────────────────────────────────

def fetch_adzuna() -> list[dict]:
    log.info("Fetching Adzuna...")
    jobs = []
    queries = [
        "data analyst SQL", "product analyst", "product analytics",
        "analytics engineer", "data scientist product", "growth analyst",
        "business operations analyst", "bizops analyst",
        "strategy analyst", "operations analyst",
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
                }, "adzuna"))

        except Exception as e:
            log.warning(f"Adzuna query '{q}' failed: {e}")

    log.info(f"Adzuna: {len(jobs)} raw jobs")
    return jobs

# ── ATS fetchers ─────────────────────────────────────────────────────────────

def fetch_greenhouse(slug: str) -> list[dict]:
    if slug in DEAD_GREENHOUSE_SLUGS:
        return []

    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
            timeout=10,
        )
        if r.status_code in (404, 410):
            return []

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
        r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=10)
        if r.status_code == 404:
            return []

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

# ── Stage 2: Objective filtering ─────────────────────────────────────────────

def is_valid_job(job: dict) -> tuple[bool, str | None]:
    title = (job.get("title") or "").lower()
    desc  = (job.get("description") or "").lower()
    text  = f"{title} {desc}"

    if not job.get("title") or not job.get("url"):
        return False, "missing_title_or_url"

    if "engineer" in title:
        return False, "engineer_title"

    if not ("sql" in text or "python" in text):
        return False, "no_sql_or_python"

    # ── location filter (NEW) ──
    if not passes_location_filter(job):
        return False, "location_filter"

    return True, None

# ── Stage 3: Claude evaluation ────────────────────────────────────────────────

def evaluate_job(client: Anthropic, job: dict) -> dict | None:
    prompt = f"""
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Source: {job['source']}
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

    except Exception:
        return None

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Job Scout starting ===")

    seen_jobs = load_set(SEEN_JOBS_FILE)
    seen_yc   = load_set(SEEN_YC_SLUGS_FILE)

    all_jobs = []
    all_jobs += fetch_adzuna()

    for slug in GREENHOUSE_COMPANIES:
        all_jobs += fetch_greenhouse(slug)

    for slug in LEVER_COMPANIES:
        all_jobs += fetch_lever(slug)

    for slug in WORKABLE_COMPANIES:
        all_jobs += fetch_workable(slug)

    new_jobs = []
    for job in all_jobs:
        if job_id(job) in seen_jobs:
            continue
        ok, _ = is_valid_job(job)
        if ok:
            new_jobs.append(job)

    log.info(f"Passed filters: {len(new_jobs)}")

    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    matches = []

    for job in new_jobs:
        result = evaluate_job(client, job)
        seen_jobs.add(job_id(job))
        if result and result.get("recommend"):
            matches.append((job, result))

    save_set(SEEN_JOBS_FILE, seen_jobs)

    log.info(f"Matches: {len(matches)}")
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
