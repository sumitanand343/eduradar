"""
fetch.py — EduRadar
Reads sources.yaml, fetches RSS/Atom feeds, classifies stories
into streams using keyword rules, deduplicates, and writes:
  - data/stories.json   (consumed by the HTML template)
  - data/last_run.txt   (timestamp shown in the footer)

Run:  python fetch.py
Deps: pip install requests feedparser pyyaml
"""

import json
import os
import sqlite3
import time
import urllib.robotparser
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import yaml

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
DB_PATH    = DATA_DIR / "stories.db"
JSON_PATH  = DATA_DIR / "stories.json"
LAST_RUN   = DATA_DIR / "last_run.txt"
SOURCES    = BASE_DIR / "sources.yaml"
MAX_STORIES_PER_STREAM = 60   # kept in the JSON
MAX_AGE_DAYS = 60             # older stories are dropped
USER_AGENT   = "EduRadar/1.0 (+https://github.com/sumitanand343/eduradar)"

# ── Streams ───────────────────────────────────────────────────────────────────
STREAMS = {
    "ai_education":  "AI in Education",
    "labor":         "Skills & Labor Market",
    "tvet":          "TVET & Workforce",
    "policy":        "Policy",
    "research":      "Research & Evidence",
    "opportunities": "Opportunities",
}

# Keyword rules — order matters; first match wins
KEYWORD_RULES: list[tuple[str, list[str]]] = [
    ("opportunities", [
        "grant", "call for proposals", "rfp", "rfq", "vacancy",
        "job opening", "fellowship", "scholarship", "tender",
    ]),
    ("research", [
        "randomized", "rct", "evaluation", "working paper", "evidence",
        "impact study", "meta-analysis", "systematic review", "preprint",
        "arxiv", "journal", "dissertation", "replication",
    ]),
    ("tvet", [
        "tvet", "vocational", "apprenticeship", "technical education",
        "workforce training", "upskilling", "reskilling", "national qualifications",
        "skills framework", "competency-based", "technical and vocational",
        "community college", "polytechnic",
    ]),
    ("labor", [
        "labor market", "labour market", "future of work", "skills demand",
        "job displacement", "automation", "workforce", "employment",
        "unemployment", "wage", "human capital", "skills gap",
        "linkedin economic graph", "lightcast", "burning glass",
    ]),
    ("policy", [
        "policy", "strategy", "regulation", "legislation", "ministry",
        "government", "reform", "national plan", "framework",
        "curriculum", "accreditation", "funding",
    ]),
    ("ai_education", [
        "ai in education", "edtech", "artificial intelligence",
        "machine learning", "large language model", "llm", "chatgpt",
        "generative ai", "personalized learning", "adaptive learning",
        "learning analytics", "khan academy", "duolingo", "classroom",
        "teacher", "student", "school", "university", "higher education",
        "digital learning", "e-learning", "elearning", "online learning",
    ]),
]


# ── Robots.txt cache ──────────────────────────────────────────────────────────
_robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}

def can_fetch(url: str) -> bool:
    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    if base not in _robots_cache:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            # If robots.txt is unreachable, assume allowed
            _robots_cache[base] = None
            return True
        _robots_cache[base] = rp
    rp = _robots_cache[base]
    if rp is None:
        return True
    return rp.can_fetch(USER_AGENT, url)


# ── Database ──────────────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stories (
            id          TEXT PRIMARY KEY,
            title       TEXT,
            url         TEXT,
            summary     TEXT,
            source      TEXT,
            stream      TEXT,
            published   TEXT,
            fetched_at  TEXT
        )
    """)
    conn.commit()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


def upsert_story(conn: sqlite3.Connection, story: dict) -> bool:
    """Returns True if this was a new story."""
    existing = conn.execute(
        "SELECT id FROM stories WHERE id = ?", (story["id"],)
    ).fetchone()
    if existing:
        return False
    conn.execute(
        """INSERT INTO stories (id, title, url, summary, source, stream, published, fetched_at)
           VALUES (:id, :title, :url, :summary, :source, :stream, :published, :fetched_at)""",
        story,
    )
    conn.commit()
    return True


def prune_old(conn: sqlite3.Connection) -> None:
    cutoff = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    conn.execute("DELETE FROM stories WHERE published < ?", (cutoff_str,))
    conn.commit()


# ── Classification ────────────────────────────────────────────────────────────
def classify(title: str, summary: str, hint: str) -> str:
    text = (title + " " + summary).lower()
    for stream, keywords in KEYWORD_RULES:
        if any(kw in text for kw in keywords):
            return stream
    # Fall back to the source hint if keywords miss
    return hint if hint in STREAMS else "ai_education"


# ── Fetch ─────────────────────────────────────────────────────────────────────
def parse_date(entry) -> str:
    """Return an ISO-8601 UTC string from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                ts = time.mktime(val)
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


def fetch_feed(source: dict) -> list[dict]:
    url  = source["url"]
    hint = source.get("hint", "ai_education")
    name = source["name"]

    if not can_fetch(url):
        print(f"  [robots] blocked: {url}")
        return []

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=20,
        )
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [fetch error] {name}: {exc}")
        return []

    feed    = feedparser.parse(resp.text)
    stories = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for entry in feed.entries:
        link    = getattr(entry, "link", None)
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
        # Strip basic HTML tags from summary
        import re
        summary = re.sub(r"<[^>]+>", " ", summary).strip()
        summary = " ".join(summary.split())[:500]  # cap at 500 chars

        if not link or not title:
            continue

        sid     = story_id(link)
        stream  = classify(title, summary, hint)
        pub     = parse_date(entry)

        stories.append({
            "id":         sid,
            "title":      title.strip(),
            "url":        link,
            "summary":    summary,
            "source":     name,
            "stream":     stream,
            "published":  pub,
            "fetched_at": now_iso,
        })

    print(f"  [ok] {name}: {len(stories)} entries")
    return stories


# ── Export JSON ───────────────────────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> None:
    cutoff = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()

    result = {}
    for stream_key in STREAMS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published
               FROM stories
               WHERE stream = ? AND published > ?
               ORDER BY published DESC
               LIMIT ?""",
            (stream_key, cutoff_str, MAX_STORIES_PER_STREAM),
        ).fetchall()
        result[stream_key] = [
            {
                "title":     r[0],
                "url":       r[1],
                "summary":   r[2],
                "source":    r[3],
                "published": r[4],
            }
            for r in rows
        ]

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    LAST_RUN.write_text(ts)
    print(f"\nWrote {JSON_PATH} — {ts}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    sources_raw = yaml.safe_load(SOURCES.read_text())["sources"]

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    new_count = 0
    for source in sources_raw:
        print(f"Fetching: {source['name']}")
        stories = fetch_feed(source)
        for s in stories:
            if upsert_story(conn, s):
                new_count += 1
        time.sleep(1)  # polite pause between requests

    prune_old(conn)
    build_json(conn)
    conn.close()

    print(f"\nDone. {new_count} new stories added.")


if __name__ == "__main__":
    main()
