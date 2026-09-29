"""
fetch.py — EduRadar
Parallel fetcher: uses 10 concurrent workers so all 150+ sources
complete in under 15 minutes instead of 6+ hours.

Reads sources.yaml, fetches RSS/Atom feeds, applies an education
gate to AI/tech sources, classifies stories into 12 streams and
11 regions, deduplicates, and writes:
  - data/stories.json
  - data/last_run.txt

Run:  python fetch.py
Deps: pip install requests feedparser pyyaml
"""

import json
import re
import sqlite3
import threading
import time
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import yaml

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR               = Path(__file__).parent
DATA_DIR               = BASE_DIR / "data"
DB_PATH                = DATA_DIR / "stories.db"
JSON_PATH              = DATA_DIR / "stories.json"
LAST_RUN               = DATA_DIR / "last_run.txt"
SOURCES                = BASE_DIR / "sources.yaml"
MAX_STORIES_PER_STREAM = 80
MAX_AGE_DAYS           = 60
MAX_WORKERS            = 10      # parallel fetches
TIMEOUT                = 8       # seconds per request — was 20, caused 6h timeouts

# Standard browser user agent — avoids most robots blocks
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ROBOTS_AGENT = "Mozilla"

# ── Streams ───────────────────────────────────────────────────────────────────
STREAMS = {
    "fln":       "Foundational Learning",
    "ece":       "Early Childhood",
    "k12":       "K-12 Education",
    "secondary": "Secondary Education",
    "higher":    "Higher Education",
    "tvet":      "TVET & Vocational",
    "labor":     "Skills & Labor Market",
    "teacher":   "Teacher Development",
    "edtech":    "EdTech & AI Tools",
    "policy":    "Policy & Governance",
    "research":  "Research & Evidence",
    "funding":   "Funding & Opportunities",
}

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

# ── Education gate ────────────────────────────────────────────────────────────
EDUCATION_GATED_SOURCES = {
    "arXiv — cs.CY (Computers & Society)",
    "OpenAI — News & Research",
    "Anthropic — News & Research",
    "Google — The Keyword Blog",
    "Google DeepMind — Blog",
    "Google Research — Blog",
    "Microsoft Research — Blog",
    "Meta AI — Blog",
    "MIT Technology Review — AI",
    "Allen Institute for AI (AI2)",
    "Stanford HAI — Human-Centered AI",
    "AI Now Institute",
    "Future of Life Institute — AI & Education",
    "Partnership on AI — Blog",
    "BBC — News",
    "The Guardian — World",
    "Financial Times",
    "The Economist",
    "Reuters",
    "Al Jazeera — News",
}

EDUCATION_GATE_KEYWORDS = [
    "education", "learning", "school", "student", "teacher", "classroom",
    "literacy", "numeracy", "curriculum", "pedagogy", "university", "college",
    "skill", "training", "workforce", "vocational", "tvet", "edtech",
    "tutoring", "assessment", "teaching", "academic", "course", "degree",
    "scholarship", "campus", "faculty", "dropout", "enrollment",
    "homework", "lecture", "exam", "lesson", "instruction",
    "early childhood", "k-12", "higher ed", "upskill", "reskill",
    "apprenticeship", "foundational", "reading", "math", "stem",
    "tutor", "child development", "adult learning", "lifelong learning",
    "distance learning", "blended learning", "learning outcome",
    "learning loss", "edtech", "future of work", "labour market",
    "labor market", "jobs", "employment", "unemployment", "skills gap",
    "reskilling", "upskilling", "human capital", "ai in education",
    "artificial intelligence education", "digital skills",
]


def passes_education_gate(source_name: str, title: str, summary: str) -> bool:
    if source_name not in EDUCATION_GATED_SOURCES:
        return True
    text = (title + " " + summary).lower()
    return any(kw in text for kw in EDUCATION_GATE_KEYWORDS)


# ── Keyword classification — first match wins ─────────────────────────────────
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
        "learning assessment", "egra", "egma",
    ]),
    ("fln", [
        "foundational literacy", "foundational numeracy", "fln",
        "early grade reading", "early grade math", "egra", "egma",
        "teaching at the right level", "tarl", "numeracy",
        "reading skills", "basic literacy", "pratham", "room to read",
    ]),
    ("ece", [
        "early childhood", "pre-primary", "preschool", "kindergarten",
        "ece", "ecd", "early learning", "nursery", "child development",
        "early years",
    ]),
    ("tvet", [
        "tvet", "vocational", "apprenticeship", "technical education",
        "workforce training", "skills framework", "competency-based",
        "technical and vocational", "community college", "polytechnic",
        "trade training",
    ]),
    ("labor", [
        "labor market", "labour market", "future of work", "skills demand",
        "job displacement", "automation", "employment", "unemployment",
        "wage", "human capital", "skills gap", "workforce development",
        "lightcast", "burning glass", "reskilling", "upskilling",
        "digital skills", "green skills",
    ]),
    ("teacher", [
        "teacher training", "teacher education", "professional development",
        "pedagogy", "teaching quality", "teacher workforce",
        "in-service training", "pre-service", "teacher support",
        "instructional coaching",
    ]),
    ("edtech", [
        "edtech", "artificial intelligence in education",
        "ai in education", "ai in learning", "ai in schools",
        "ai for education", "personalized learning", "adaptive learning",
        "learning analytics", "khan academy", "duolingo",
        "digital learning", "e-learning", "elearning", "online learning",
        "learning platform", "lms", "intelligent tutoring", "ai tutor",
        "generative ai education", "llm education", "ed-tech",
    ]),
    ("secondary", [
        "secondary school", "secondary education", "high school",
        "upper secondary", "lower secondary", "a-level", "gcse",
        "baccalaureate", "adolescent education",
    ]),
    ("higher", [
        "university", "higher education", "college", "undergraduate",
        "postgraduate", "phd", "master's", "faculty", "campus",
        "degree", "enrollment", "tuition",
    ]),
    ("k12", [
        "primary school", "elementary school", "k-12", "k12",
        "basic education", "primary education", "school enrollment",
        "out of school", "dropout", "attendance",
    ]),
    ("policy", [
        "policy", "strategy", "regulation", "legislation", "ministry",
        "government", "reform", "national plan", "curriculum",
        "accreditation", "education system", "governance",
    ]),
]


# ── Robots.txt — thread-safe ──────────────────────────────────────────────────
_robots_cache: dict = {}
_robots_lock = threading.Lock()


def can_fetch(url: str) -> bool:
    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    with _robots_lock:
        if base not in _robots_cache:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{base}/robots.txt")
            try:
                rp.read()
                _robots_cache[base] = rp
            except Exception:
                _robots_cache[base] = None
        rp = _robots_cache[base]
    return True if rp is None else rp.can_fetch(ROBOTS_AGENT, url)


# ── Database — thread-safe writes ────────────────────────────────────────────
_db_lock = threading.Lock()


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
    try:
        conn.execute("ALTER TABLE stories ADD COLUMN region TEXT DEFAULT 'global'")
    except Exception:
        pass
    conn.commit()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


def upsert_stories_batch(conn: sqlite3.Connection, stories: list[dict]) -> int:
    """Insert all new stories in one transaction. Returns count of new ones."""
    new = 0
    with _db_lock:
        for story in stories:
            existing = conn.execute(
                "SELECT id FROM stories WHERE id = ?", (story["id"],)
            ).fetchone()
            if not existing:
                conn.execute(
                    """INSERT INTO stories
                       (id,title,url,summary,source,stream,region,published,fetched_at)
                       VALUES (:id,:title,:url,:summary,:source,:stream,:region,:published,:fetched_at)""",
                    story,
                )
                new += 1
        conn.commit()
    return new


def prune_old(conn: sqlite3.Connection) -> None:
    cutoff     = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
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


# ── Fetch one feed ────────────────────────────────────────────────────────────
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


def fetch_feed(source: dict) -> tuple[str, list[dict], str]:
    """Returns (source_name, stories, status_message)."""
    url    = source["url"]
    hint   = source.get("hint", "policy")
    region = source.get("region", "global")
    name   = source["name"]

    if not can_fetch(url):
        return name, [], "robots blocked"

    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": HTTP_USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as exc:
        short = str(exc)[:80]
        return name, [], f"error: {short}"

    feed    = feedparser.parse(resp.text)
    now_iso = datetime.now(timezone.utc).isoformat()
    stories = []
    gated   = 0

    for entry in feed.entries:
        link    = getattr(entry, "link", None)
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
        summary = re.sub(r"<[^>]+>", " ", summary).strip()
        summary = " ".join(summary.split())[:600]

        if not link or not title:
            continue

        if not passes_education_gate(name, title, summary):
            gated += 1
            continue

        stories.append({
            "id":         story_id(link),
            "title":      title.strip(),
            "url":        link,
            "summary":    summary,
            "source":     name,
            "stream":     classify(title, summary, hint),
            "region":     region,
            "published":  parse_date(entry),
            "fetched_at": now_iso,
        })

    msg = f"{len(stories)} entries"
    if gated:
        msg += f", {gated} filtered by education gate"
    return name, stories, msg


# ── Export JSON ───────────────────────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> None:
    cutoff     = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    cutoff_str = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    result     = {"streams": {}, "regions": {}}

    for stream_key in STREAMS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, region
               FROM stories WHERE stream = ? AND published > ?
               ORDER BY published DESC LIMIT ?""",
            (stream_key, cutoff_str, MAX_STORIES_PER_STREAM),
        ).fetchall()
        result["streams"][stream_key] = [
            {"title": r[0], "url": r[1], "summary": r[2],
             "source": r[3], "published": r[4], "region": r[5],
             "stream": stream_key}
            for r in rows
        ]

    for region_key in REGIONS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, stream
               FROM stories WHERE region = ? AND published > ?
               ORDER BY published DESC LIMIT 40""",
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
    conn        = sqlite3.connect(DB_PATH, check_same_thread=False)
    init_db(conn)

    print(f"Fetching {len(sources_raw)} sources with {MAX_WORKERS} parallel workers...\n")
    t0        = time.time()
    new_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_feed, src): src for src in sources_raw}
        for future in as_completed(futures):
            name, stories, msg = future.result()
            print(f"  [{msg}] {name}")
            if stories:
                new_count += upsert_stories_batch(conn, stories)

    elapsed = time.time() - t0
    print(f"\nAll sources fetched in {elapsed:.0f}s")

    prune_old(conn)
    build_json(conn)
    conn.close()
    print(f"Done. {new_count} new stories added.")


if __name__ == "__main__":
    main()
