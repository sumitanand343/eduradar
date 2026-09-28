# EduSkills AI Pulse

A live, auto-updating feed for everything happening at the intersection of **AI, education, labor market skills, and TVET** — updated every 6 hours, hosted free on GitHub Pages.

## How it works

1. `fetch.py` reads ~50 RSS feeds listed in `sources.yaml`.
2. Each story is classified into one of six streams using keyword rules — no paid AI API required.
3. Stories are stored in a local SQLite database (`data/stories.db`) and exported to `data/stories.json`.
4. `index.html` reads the JSON and renders a filterable, searchable card grid.
5. A GitHub Actions workflow runs the fetcher every 6 hours and deploys the result to GitHub Pages automatically.

## Streams

| Stream | What it covers |
|--------|---------------|
| AI in Education | EdTech tools, product launches, classroom AI, adaptive learning |
| Skills & Labor Market | Future of work, skills demand, job data, employer surveys |
| TVET & Workforce | Vocational systems, apprenticeships, upskilling programs |
| Policy | National AI-in-education guidance, TVET reforms, digital skills strategies |
| Research & Evidence | RCTs, working papers, evaluations, reports |
| Opportunities | Grants, calls for proposals, fellowships, jobs |

## Setup

### 1. Fork this repo

### 2. Enable GitHub Pages
Go to **Settings → Pages** and set the source to the `gh-pages` branch (created automatically by the workflow).

### 3. Enable Actions
Go to **Settings → Actions → General** and allow workflow runs. The workflow needs `contents: write` permission — confirm this under **Settings → Actions → General → Workflow permissions**.

### 4. Run manually first
Go to **Actions → Fetch & Deploy → Run workflow** to populate `data/stories.json` before the first scheduled run.

### 5. Customise sources
Edit `sources.yaml` to add, remove, or swap RSS feeds. Each entry has:
- `name`: display label
- `url`: RSS/Atom feed URL
- `type`: `rss` (all feeds currently use standard RSS/Atom)
- `hint`: fallback stream if keyword rules don't match

### 6. Customise keyword rules
Edit the `KEYWORD_RULES` list in `fetch.py`. Rules are evaluated in order; first match wins.

## File structure

```
├── fetch.py            # fetcher + classifier + SQLite writer
├── sources.yaml        # all feed sources
├── index.html          # single-page frontend
├── data/
│   ├── stories.json    # generated — consumed by index.html
│   ├── stories.db      # generated — SQLite store
│   └── last_run.txt    # generated — timestamp shown in footer
└── .github/
    └── workflows/
        └── fetch.yml   # schedule + deploy workflow
```

## Legal / ethics

- `fetch.py` checks `robots.txt` before every fetch and skips any URL that disallows the bot.
- Summaries are the publishers' own text, truncated to 500 characters.
- Every card links to the original source.
- No stories are reproduced in full; this is a link aggregator, not a content mirror.

## Adding more sources

Good places to look for RSS feeds:
- Append `/feed`, `/rss`, `/feed.xml`, or `?format=rss` to a blog URL
- Check `<link rel="alternate" type="application/rss+xml">` in a site's HTML source
- Use [fetchrss.com](https://fetchrss.com) to generate RSS from sites that don't have it

## License

MIT
