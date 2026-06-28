# Job Scout 🔍

A daily job search engine that sources from Adzuna, Greenhouse, and Lever — then uses Claude to evaluate each posting against your personal criteria — and emails you a digest of matches.

**Runs automatically every weekday at 8am ET via GitHub Actions. Zero cost to operate.**

---

## How it works

1. **Sources** jobs from:
   - Adzuna Canada API (broad coverage, legitimate API)
   - Greenhouse career boards (direct from company ATS)
   - Lever career boards (direct from company ATS)
2. **Deduplicates** against previously seen jobs (stored via GitHub Actions cache)
3. **Evaluates** each new listing with Claude against your criteria
4. **Emails** you a digest only on days when there are matches

---

## One-time setup

### Step 1 — Get your API keys

**Adzuna** (free):
1. Go to https://developer.adzuna.com/ and sign up
2. Create an app — you'll get an `App ID` and `App Key`

**Anthropic** (you already have this):
- Go to https://console.anthropic.com/settings/keys
- Create an API key

**Gmail App Password** (for sending emails):
1. Go to your Google Account → Security
2. Enable 2-Step Verification if not already on
3. Go to Security → "App passwords" (search for it if needed)
4. Create a new app password — name it "Job Scout"
5. Copy the 16-character password (you won't see it again)

---

### Step 2 — Create your GitHub repo

```bash
# In your terminal:
mkdir job-scout && cd job-scout
git init
git remote add origin https://github.com/YOUR_USERNAME/job-scout.git
```

Copy all the files from this project into the folder, then:
```bash
git add .
git commit -m "Initial Job Scout setup"
git push -u origin main
```

---

### Step 3 — Add secrets to GitHub

In your GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**

Add each of these:

| Secret name | Value |
|---|---|
| `ADZUNA_APP_ID` | Your Adzuna App ID |
| `ADZUNA_APP_KEY` | Your Adzuna App Key |
| `ANTHROPIC_API_KEY` | Your Anthropic API key |
| `GMAIL_SENDER` | Your Gmail address (e.g. `you@gmail.com`) |
| `GMAIL_RECIPIENT` | Where to send digests (can be same as sender) |
| `GMAIL_APP_PASSWORD` | The 16-char Gmail app password from Step 1 |

---

### Step 4 — Test it manually

Once your repo is pushed and secrets are set:
1. Go to your repo on GitHub
2. Click **Actions** tab
3. Click **Daily Job Scout** in the left sidebar
4. Click **Run workflow** → **Run workflow**

Watch the logs live. You should see it fetching, evaluating, and (if matches found) sending.

---

## Customizing your criteria

Edit the `MATCHING_CRITERIA` string in `src/scout.py` — it's plain English instructions to Claude. You can add or remove anything:

```python
MATCHING_CRITERIA = """
...your criteria here...
"""
```

## Adding companies

Add slugs to `GREENHOUSE_COMPANIES` or `LEVER_COMPANIES` in `src/scout.py`.

To find a company's slug:
- **Greenhouse**: visit `https://boards.greenhouse.io/COMPANY_SLUG` (try the company name lowercase, no spaces)
- **Lever**: visit `https://jobs.lever.co/COMPANY_SLUG`

If the page loads with job listings, the slug is correct.

---

## Cost estimate

- **Adzuna**: free (up to 250 calls/month on free tier)
- **GitHub Actions**: free (well within 2,000 min/month free tier)
- **Anthropic API**: ~$0.003 per job evaluated (claude-sonnet-4-6). If it evaluates 100 new jobs/day, that's ~$0.30/day → ~$9/month worst case. In practice much less since most days have fewer new listings.
- **Gmail**: free

---

## File structure

```
job-scout/
├── src/
│   └── scout.py          # Main script
├── data/
│   └── seen_jobs.json    # Auto-generated, tracks processed jobs
├── .github/
│   └── workflows/
│       └── daily-scout.yml  # GitHub Actions schedule
├── requirements.txt
└── README.md
```
