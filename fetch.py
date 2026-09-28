"""
fetch_v2.py — EduRadar
Reads sources_v2.yaml, fetches RSS/Atom feeds, classifies stories
into 12 streams and 11 regions, deduplicates, and writes:
  - data/stories.json
  - data/last_run.txt

Run:  python fetch_v2.py
Deps: pip install requests feedparser pyyaml
"""

import json
import os
import re
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
SOURCES    = BASE_DIR / "sources_v2.yaml"
MAX_STORIES_PER_STREAM = 80
MAX_AGE_DAYS = 60
USER_AGENT   = "EduRadar/2.0 (+https://github.com/sumitanand343/eduradar)"

# ── Streams ───────────────────────────────────────────────────────────────────
STREAMS = {
    "fln":      "Foundational Learning",
    "ece":      "Early Childhood",
    "k12":      "K-12 Education",
    "secondary":"Secondary Education",
    "higher":   "Higher Education",
    "tvet":     "TVET & Vocational",
    "labor":    "Skills & Labor Market",
    "teacher":  "Teacher Development",
    "edtech":   "EdTech & AI Tools",
    "policy":   "Policy & Governance",
    "research": "Research & Evidence",
    "funding":  "Funding & Opportunities",
}

# ── Regions ───────────────────────────────────────────────────────────────────
REGIONS = {
    "global":            "Global",
    "south_asia":        "South Asia",
    "east_africa":       "East Africa",
    "west_africa":       "West Africa",
    "mena":              "Middle East & North Africa",
    "southeast_asia":    "Southeast Asia",
    "latin_america":     "Latin America",
    "east_asia":         "East Asia",
    "europe":            "Europe",
    "north_america":     "North America",
    "australia_pacific": "Australia & Pacific",
}

# ── Keyword rules — first match wins ─────────────────────────────────────────
KEYWORD_RULES: list[tuple[str, list[str]]] = [
    ("funding", [
        "grant", "call for proposals", "rfp", "rfq", "vacancy",
        "fellowship", "scholarship", "tender", "funding opportunity",
        "job opening", "apply now", "applications open",
    ]),
    ("research", [
        "randomized", "rct", "evaluation", "working paper", "evidence",
        "impact study", "meta-analysis", "systematic review", "preprint",
        "arxiv", "journal", "dissertation", "replication", "endline",
        "baseline study", "learning assessment", "egra", "egma",
    ]),
    ("fln", [
        "foundational literacy", "foundational numeracy", "fln",
        "early grade reading", "early grade math", "egra", "egma",
        "teaching at the right level", "tarl", "numeracy", "literacy",
        "reading skills", "basic literacy", "learning outcomes",
        "pratham", "room to read", "uwezo",
    ]),
    ("ece", [
        "early childhood", "pre-primary", "preschool", "kindergarten",
        "ece", "ecd", "early learning", "nursery", "playgroup",
        "child development", "early years",
    ]),
    ("tvet", [
        "tvet", "vocational", "apprenticeship", "technical education",
        "workforce training", "skills framework", "competency-based",
        "technical and vocational", "community college", "polytechnic",
        "cedefop", "etf", "ncver", "trade training",
    ]),
    ("labor", [
        "labor market", "labour market", "future of work", "skills demand",
        "job displacement", "automation", "employment", "unemployment",
        "wage", "human capital", "skills gap", "linkedin economic graph",
        "lightcast", "burning glass", "workforce development",
    ]),
    ("teacher", [
        "teacher training", "teacher education", "professional development",
        "pedagogy", "teaching quality", "teacher workforce",
        "in-service training", "pre-service", "teacher support",
        "instructional coaching", "classroom practice",
    ]),
    ("edtech", [
        "edtech", "artificial intelligence", "machine learning",
        "large language model", "llm", "chatgpt", "generative ai",
        "personalized learning", "adaptive learning", "learning analytics",
        "khan academy", "duolingo", "digital learning", "e-learning",
        "elearning", "online learning", "ai in education", "ed-tech",
        "learning platform", "learning management", "lms",
    ]),
    ("secondary", [
        "secondary school", "secondary education", "high school",
        "upper secondary", "lower secondary", "grade 9", "grade 10",
        "grade 11", "grade 12", "a-level", "gcse", "baccalaureate",
        "adolescent", "teenager",
    ]),
    ("higher", [
        "university", "higher education", "college", "undergraduate",
        "postgraduate", "phd", "master", "academic", "faculty",
        "campus", "degree", "enrollment", "tuition",
    ]),
    ("k12", [
        "primary school", "elementary school", "k-12", "k12",
        "basic education", "primary education", "school enrollment",
        "out of school", "dropout", "attendance", "school feeding",
    ]),
    ("policy", [
        "policy", "strategy", "regulation", "legislation", "ministry",
        "government", "reform", "national plan", "curriculum",
        "accreditation", "education system", "governance",
    ]),
]


# ── Robots.txt ────────────────────────────────────────────────────────────────
_robots_cache: dict = {}

def can_fetch(url: str) -> bool:
    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    if base not in _robots_cache:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
            _robots_cache[base] = rp
        except Exception:
            _robots_cache[base] = None
    rp = _robots_cache[base]
    return True if rp is None else rp.can_fetch(USER_AGENT, url)


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
            region      TEXT,
            published   TEXT,
            fetched_at  TEXT
        )
    """)
    # Add region column if upgrading from v1
    try:
        conn.execute("ALTER TABLE stories ADD COLUMN region TEXT DEFAULT 'global'")
    except Exception:
        pass
    conn.commit()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


def upsert_story(conn: sqlite3.Connection, story: dict) -> bool:
    existing = conn.execute(
        "SELECT id FROM stories WHERE id = ?", (story["id"],)
    ).fetchone()
    if existing:
        return False
    conn.execute(
        """INSERT INTO stories
           (id, title, url, summary, source, stream, region, published, fetched_at)
           VALUES (:id, :title, :url, :summary, :source, :stream, :region, :published, :fetched_at)""",
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
    return hint if hint in STREAMS else "policy"


# ── Fetch ─────────────────────────────────────────────────────────────────────
def parse_date(entry) -> str:
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
    url    = source["url"]
    hint   = source.get("hint", "policy")
    region = source.get("region", "global")
    name   = source["name"]

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
        print(f"  [error] {name}: {exc}")
        return []

    feed    = feedparser.parse(resp.text)
    now_iso = datetime.now(timezone.utc).isoformat()
    stories = []

    for entry in feed.entries:
        link    = getattr(entry, "link", None)
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
        summary = re.sub(r"<[^>]+>", " ", summary).strip()
        summary = " ".join(summary.split())[:600]

        if not link or not title:
            continue

        stream = classify(title, summary, hint)

        stories.append({
            "id":         story_id(link),
            "title":      title.strip(),
            "url":        link,
            "summary":    summary,
            "source":     name,
            "stream":     stream,
            "region":     region,
            "published":  parse_date(entry),
            "fetched_at": now_iso,
        })

    print(f"  [ok] {name}: {len(stories)} entries")
    return stories


# ── Export JSON ───────────────────────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> None:
    cutoff = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()

    result = {"streams": {}, "regions": {}}

    # By stream
    for stream_key in STREAMS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, region
               FROM stories
               WHERE stream = ? AND published > ?
               ORDER BY published DESC
               LIMIT ?""",
            (stream_key, cutoff_str, MAX_STORIES_PER_STREAM),
        ).fetchall()
        result["streams"][stream_key] = [
            {"title": r[0], "url": r[1], "summary": r[2],
             "source": r[3], "published": r[4], "region": r[5],
             "stream": stream_key}
            for r in rows
        ]

    # By region (top 40 per region, any stream)
    for region_key in REGIONS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, stream
               FROM stories
               WHERE region = ? AND published > ?
               ORDER BY published DESC
               LIMIT 40""",
            (region_key, cutoff_str),
        ).fetchall()
        result["regions"][region_key] = [
            {"title": r[0], "url": r[1], "summary": r[2],
             "source": r[3], "published": r[4], "stream": r[5],
             "region": region_key}
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
        time.sleep(1)

    prune_old(conn)
    build_json(conn)
    conn.close()
    print(f"\nDone. {new_count} new stories added.")


if __name__ == "__main__":
    main()
