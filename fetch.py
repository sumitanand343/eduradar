"""
fetch.py — EduRadar

Reads sources.yaml, fetches every RSS/Atom feed in parallel, applies
a strict education gate ONLY to general media and AI lab sources,
classifies stories into 10 streams, and writes:
  - data/stories.json
  - data/last_run.txt
  - data/stories.db

Run:  python fetch.py
Deps: pip install requests feedparser pyyaml
"""

import calendar
import html
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import yaml

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
DB_PATH   = DATA_DIR / "stories.db"
JSON_PATH = DATA_DIR / "stories.json"
LAST_RUN  = DATA_DIR / "last_run.txt"
SOURCES   = BASE_DIR / "sources.yaml"

MAX_STORIES_PER_STREAM = 80
MAX_AGE_DAYS           = 60
MAX_WORKERS            = 10
CONNECT_TIMEOUT        = 5
READ_TIMEOUT           = 10
TOTAL_TIMEOUT          = 20
MAX_BYTES              = 5_000_000
ROBOTS_TIMEOUT         = 5
FETCH_DEADLINE         = 15 * 60

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ROBOTS_AGENT = "Mozilla"
HEADERS = {
    "User-Agent": HTTP_USER_AGENT,
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
}

# ── Streams — 10 streams ──────────────────────────────────────────────────────
STREAMS = {
    "foundational":  "Foundational & Early Learning",
    "school":        "School Education",
    "higher":        "Higher Education",
    "tvet":          "TVET & Vocational",
    "labor":         "Skills & Labor Market",
    "teacher":       "Teacher Development",
    "edtech":        "EdTech & AI Tools",
    "policy":        "Policy & Governance",
    "research":      "Research & Evidence",
    "funding":       "Funding & Opportunities",
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

# ── Domain blocklist ──────────────────────────────────────────────────────────
BLOCKED_DOMAINS = {
    "developmentpathways.co.uk",
    "opportunitydesk.org",
    "ideas.repec.org",
}

# ── Education gate — ONLY for these general/AI sources ───────────────────────
# All education-specific orgs (UNESCO, Pratham, OECD, NORRAG, etc.)
# are NOT listed here and bypass the gate entirely.
# Only broad media and AI labs need the title gate.
EDUCATION_GATED_SOURCES = {
    # General media
    "BBC — Business & Work",
    "The Guardian — Technology",
    "New York Times — Technology",
    "Al Jazeera — News (education-gated)",
    "Financial Times — Education",
    "The Economist — Education",
    "The Independent — Education",
    # AI labs — only education-relevant stories
    "Google — The Keyword Blog",
    "Google DeepMind — Blog",
    "OpenAI — News & Research",
    "Anthropic — News & Research",
    "MIT Technology Review — AI",
    "Allen Institute for AI (AI2)",
    "Partnership on AI — Blog",
    "AI Now Institute",
    "Future of Life Institute",
    "Stanford HAI — Human-Centered AI",
    # WEF — broad agenda
    "WEF — Forum Stories",
    "WEF — Agenda",
    # General donor/development news
    "World Bank — News (education-gated)",
    "JICA (Japan) — Press Releases",
    "GIZ (Germany) — Development News",
    "Australian DFAT — Development",
    "Norad (Norway) — Aid Results",
    # General newspapers
    "Dawn — Pakistan",
    "The Daily Star — Bangladesh",
    "Al-Fanar Media — Arab Higher Education",
    "Nation Africa — Kenya",
    "African Arguments — Development",
    "Jeune Afrique — Societe",
    "Rest of World",
    "Buenos Aires Times — Argentina",
}

# Title must contain at least one of these for gated sources
TITLE_EDU_KEYWORDS = [
    "education", "educational", "school", "schooling", "student", "pupil",
    "teacher", "teaching", "classroom", "curriculum", "literacy", "numeracy",
    "learning", "learner", "university", "college", "campus", "degree",
    "dropout", "enrollment", "enrolment", "exam", "lecture", "lesson",
    "tutor", "tutoring", "textbook", "scholarship", "fellowship",
    "kindergarten", "preschool", "early childhood", "k-12", "edtech",
    "ed-tech", "skill", "upskilling", "reskilling", "vocational", "tvet",
    "apprenticeship", "workforce", "labour market", "labor market",
    "future of work", "job market", "employment", "unemployment",
    "human capital", "digital skills", "ai in education", "ai in schools",
    "ai in learning", "ai for education", "ai tutor", "ai literacy",
    "generative ai", "chatgpt", "edtech", "learning outcome",
    "learning loss", "learning poverty", "foundational", "egra", "egma",
    "tarl", "aser", "teaching at the right level",
    # French / Spanish / Portuguese
    "éducation", "école", "educación", "educação", "escuela", "universidad",
    "aprendizaje", "alfabetización", "enseignement",
]
_TITLE_RE = re.compile(
    r"(?<!\w)(?:" + "|".join(re.escape(w) for w in sorted(TITLE_EDU_KEYWORDS, key=len, reverse=True)) + r")(?:s|es)?(?!\w)",
    re.IGNORECASE,
)

# ── Quality filter — drop obvious noise regardless of source ──────────────────
NOISE_RE = re.compile(
    r"\b(lost money|made \$|crypto|bitcoin|forex|casino|dating|horoscope|"
    r"celebrity|recipe|fashion|beauty tip|weight loss|diet plan|net worth|"
    r"how I made|personal story)\b",
    re.IGNORECASE,
)

# ── Stream classification — first match wins ──────────────────────────────────
def _re(words):
    alts = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(r"(?<!\w)(?:" + alts + r")(?:s|es)?(?!\w)", re.IGNORECASE)


STREAM_RULES = [
    # ── Funding first — isolated ──────────────────────────────────────────────
    ("funding", _re([
        "scholarship opportunity", "fellowship opportunity", "grant application",
        "call for proposals", "request for proposals", "rfp", "rfq",
        "job opening", "job posting", "vacancy", "vacancies",
        "apply now", "applications open", "deadline to apply",
        "tender", "funding opportunity", "funded programme",
    ])),

    # ── Research & Evidence ───────────────────────────────────────────────────
    ("research", _re([
        "randomized controlled", "randomised controlled", "rct",
        "impact evaluation", "working paper", "meta-analysis",
        "systematic review", "preprint", "quasi-experimental",
        "replication study", "endline survey", "baseline survey",
        "learning assessment results", "egra", "egma", "aser",
        "evidence review", "study finds", "research finds", "new evidence",
    ])),

    # ── Foundational & Early Learning (merged FLN + ECE) ─────────────────────
    ("foundational", _re([
        "foundational literacy", "foundational numeracy", "foundational learning",
        "foundational skills", "fln", "early grade reading", "early grade math",
        "teaching at the right level", "tarl", "reading level",
        "learning poverty", "basic literacy", "basic numeracy",
        "early childhood education", "early childhood development",
        "early childhood care", "pre-primary", "preschool", "pre-school",
        "kindergarten", "nursery school", "ecd", "ece",
        "child development", "early years education", "early learning",
        "play-based learning", "zero to five", "0-6 years",
    ])),

    # ── TVET & Vocational ─────────────────────────────────────────────────────
    ("tvet", _re([
        "tvet", "technical and vocational", "vocational education",
        "vocational training", "vocational school", "apprenticeship",
        "technical education", "trade school", "skills framework",
        "national qualifications framework", "competency-based training",
        "community college", "polytechnic", "vet system",
        "workforce training programme",
    ])),

    # ── Skills & Labor Market ─────────────────────────────────────────────────
    ("labor", _re([
        "labor market", "labour market", "future of work",
        "skills demand", "skills shortage", "skills mismatch",
        "job displacement", "automation of jobs", "workforce skills",
        "human capital development", "skills gap",
        "reskilling", "upskilling", "digital skills training",
        "green skills", "jobs report", "employment report",
        "youth employment", "school-to-work", "work-based learning",
        "workforce development",
    ])),

    # ── Teacher Development ───────────────────────────────────────────────────
    ("teacher", _re([
        "teacher training", "teacher education",
        "teacher professional development", "teaching quality",
        "teacher workforce", "teacher shortage", "teacher recruitment",
        "in-service training", "pre-service training",
        "teacher support", "instructional coaching",
        "pedagogical", "teaching practice",
        "teacher assessment", "teacher evaluation",
    ])),

    # ── EdTech & AI Tools ────────────────────────────────────────────────────
    ("edtech", _re([
        "edtech", "ed-tech", "ai in education", "ai in learning",
        "ai in schools", "ai for education", "ai for learning",
        "artificial intelligence in education", "ai tutor", "ai tutoring",
        "intelligent tutoring system", "ai literacy",
        "personalized learning platform", "adaptive learning system",
        "learning analytics platform", "educational technology",
        "digital learning platform", "e-learning platform",
        "online learning platform", "learning management system",
        "lms", "khanmigo", "duolingo", "chatgpt in education",
        "generative ai in education", "large language model education",
        "ai and education", "technology in education",
        "digital education", "open educational resources",
    ])),

    # ── Higher Education ──────────────────────────────────────────────────────
    ("higher", _re([
        "higher education", "university education", "college education",
        "undergraduate programme", "postgraduate programme",
        "phd programme", "doctoral research", "university policy",
        "university funding", "university ranking", "academic freedom",
        "campus", "higher education reform", "tuition fees",
        "graduate programme", "faculty research",
    ])),

    # ── School Education (K-12 + Secondary merged) ────────────────────────────
    ("school", _re([
        "primary school", "elementary school", "k-12", "k12",
        "basic education", "primary education", "school enrollment",
        "school enrolment", "out of school children", "school dropout",
        "school attendance", "school system", "school curriculum",
        "school policy", "school funding", "school leadership",
        "school infrastructure", "girls education", "girls school",
        "inclusive education", "special education",
        "secondary school", "secondary education", "high school",
        "upper secondary", "lower secondary", "middle school",
        "a-level", "gcse", "baccalaureate", "secondary curriculum",
    ])),

    # ── Policy & Governance ───────────────────────────────────────────────────
    ("policy", _re([
        "education policy", "education reform", "education strategy",
        "education legislation", "ministry of education",
        "education governance", "education budget", "education spending",
        "education regulation", "education accreditation",
        "national education plan", "education framework",
        "education system reform", "skills policy",
    ])),
]


def classify(title: str, summary: str, hint: str) -> str:
    text = f"{title} {summary}"
    for stream, pattern in STREAM_RULES:
        if pattern.search(text):
            return stream
    return hint if hint in STREAMS else "policy"


# ── Helpers ───────────────────────────────────────────────────────────────────
def is_gated(source: dict) -> bool:
    return bool(source.get("gate")) or source.get("name") in EDUCATION_GATED_SOURCES


def passes_title_gate(title: str) -> bool:
    return bool(_TITLE_RE.search(title))


def is_blocked_domain(url: str) -> bool:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    return any(domain == b or domain.endswith("." + b) for b in BLOCKED_DOMAINS)


# ── robots.txt ────────────────────────────────────────────────────────────────
_robots_cache: dict = {}
_robots_lock = threading.Lock()


def _load_robots(base: str):
    try:
        r = requests.get(f"{base}/robots.txt", headers=HEADERS,
                         timeout=(ROBOTS_TIMEOUT, ROBOTS_TIMEOUT))
    except Exception:
        return None
    if r.status_code >= 400:
        return None
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(r.text[:500_000].splitlines())
    return rp


def can_fetch(url: str) -> bool:
    p    = urlparse(url)
    base = f"{p.scheme}://{p.netloc}"
    with _robots_lock:
        if base in _robots_cache:
            rp = _robots_cache[base]
            return True if rp is None else rp.can_fetch(ROBOTS_AGENT, url)
    rp = _load_robots(base)
    with _robots_lock:
        _robots_cache[base] = rp
    return True if rp is None else rp.can_fetch(ROBOTS_AGENT, url)


# ── Download ──────────────────────────────────────────────────────────────────
def http_get(url: str) -> bytes:
    start = time.monotonic()
    with requests.get(url, headers=HEADERS, stream=True,
                      timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as r:
        r.raise_for_status()
        chunks, size = [], 0
        for chunk in r.iter_content(chunk_size=65536):
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError("feed > 5 MB")
            if time.monotonic() - start > TOTAL_TIMEOUT:
                raise TimeoutError(f"download > {TOTAL_TIMEOUT}s")
        return b"".join(chunks)


# ── Parsing ───────────────────────────────────────────────────────────────────
def clean(text: str, limit: int) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = html.unescape(text)
    return " ".join(text.split())[:limit]


def parse_date(entry) -> str:
    now = datetime.now(timezone.utc)
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        val = entry.get(attr)
        if val:
            try:
                dt = datetime.fromtimestamp(calendar.timegm(val), tz=timezone.utc)
                return min(dt, now).isoformat()
            except Exception:
                pass
    return now.isoformat()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


# ── Fetch one feed ────────────────────────────────────────────────────────────
def fetch_feed(source: dict):
    name   = source.get("name", "Unnamed")
    url    = source.get("url", "")
    hint   = source.get("hint", "policy")
    region = source.get("region", "global")
    gated  = is_gated(source)

    if is_blocked_domain(url):
        return name, [], "blocked", "domain blocklist"

    try:
        if not can_fetch(url):
            return name, [], "blocked", "robots.txt"

        feed = feedparser.parse(http_get(url))
        if not feed.entries:
            why = "not a valid RSS feed" if feed.bozo else "no items"
            return name, [], "empty", why

        now_iso = datetime.now(timezone.utc).isoformat()
        stories, dropped = [], 0

        for entry in feed.entries:
            link    = entry.get("link")
            title   = clean(entry.get("title", ""), 300)
            summary = clean(entry.get("summary") or entry.get("description") or "", 600)

            if not link or not title:
                continue

            # Drop obvious noise from any source
            if NOISE_RE.search(title):
                dropped += 1
                continue

            # Title gate — only for gated (general media + AI lab) sources
            if gated and not passes_title_gate(title):
                dropped += 1
                continue

            stories.append({
                "id":         story_id(link),
                "title":      title,
                "url":        link,
                "summary":    summary,
                "source":     name,
                "stream":     classify(title, summary, hint),
                "region":     region,
                "published":  parse_date(entry),
                "fetched_at": now_iso,
            })

        msg = f"{len(stories)} kept"
        if dropped:
            msg += f", {dropped} dropped"
        return name, stories, "ok", msg

    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return name, [], "error", f"HTTP {code}"
    except Exception as exc:
        return name, [], "error", f"{type(exc).__name__}: {str(exc)[:80]}"


# ── Database ──────────────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stories (
            id TEXT PRIMARY KEY, title TEXT, url TEXT, summary TEXT,
            source TEXT, stream TEXT, region TEXT, published TEXT, fetched_at TEXT
        )
    """)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(stories)")}
    if "region" not in cols:
        conn.execute("ALTER TABLE stories ADD COLUMN region TEXT DEFAULT 'global'")
    conn.commit()


def save_stories(conn: sqlite3.Connection, stories: list) -> int:
    before = conn.total_changes
    conn.executemany(
        """INSERT OR IGNORE INTO stories
           (id,title,url,summary,source,stream,region,published,fetched_at)
           VALUES (:id,:title,:url,:summary,:source,:stream,:region,:published,:fetched_at)""",
        stories,
    )
    conn.commit()
    return conn.total_changes - before


def cutoff_iso() -> str:
    ts = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def prune_old(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stories WHERE published < ?", (cutoff_iso(),))
    conn.commit()


def clear_db(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM stories")
    conn.commit()


# ── Export JSON ───────────────────────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> int:
    cutoff = cutoff_iso()
    result = {"streams": {}, "regions": {}, "stream_labels": STREAMS}
    total  = 0

    for key in STREAMS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, region FROM stories
               WHERE stream = ? AND published > ? ORDER BY published DESC LIMIT ?""",
            (key, cutoff, MAX_STORIES_PER_STREAM),
        ).fetchall()
        result["streams"][key] = [
            {"title": r[0], "url": r[1], "summary": r[2], "source": r[3],
             "published": r[4], "region": r[5], "stream": key}
            for r in rows
        ]
        total += len(rows)

    for key in REGIONS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, stream FROM stories
               WHERE region = ? AND published > ? ORDER BY published DESC LIMIT 40""",
            (key, cutoff),
        ).fetchall()
        result["regions"][key] = [
            {"title": r[0], "url": r[1], "summary": r[2], "source": r[3],
             "published": r[4], "stream": r[5], "region": key}
            for r in rows
        ]

    JSON_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    LAST_RUN.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                        encoding="utf-8")
    return total


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    raw = yaml.safe_load(SOURCES.read_text(encoding="utf-8"))["sources"]
    sources, seen = [], set()
    for s in raw:
        u = (s or {}).get("url")
        if u and u not in seen:
            seen.add(u)
            sources.append(s)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    print("Clearing old stories to apply new stream structure...", flush=True)
    clear_db(conn)

    print(f"Fetching {len(sources)} sources with {MAX_WORKERS} workers...\n", flush=True)
    t0     = time.monotonic()
    counts = {"ok": 0, "empty": 0, "blocked": 0, "error": 0}
    new_count, done = 0, 0

    pool    = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = [pool.submit(fetch_feed, s) for s in sources]
    try:
        for fut in as_completed(futures, timeout=FETCH_DEADLINE):
            name, stories, status, msg = fut.result()
            counts[status] += 1
            done += 1
            print(f"  [{status}] {name}: {msg}", flush=True)
            if stories:
                new_count += save_stories(conn, stories)
    except FuturesTimeout:
        print(f"\n  [deadline] {len(sources)-done} sources skipped after {FETCH_DEADLINE//60}m",
              flush=True)
    pool.shutdown(wait=False, cancel_futures=True)

    prune_old(conn)
    total = build_json(conn)
    conn.close()

    mins = (time.monotonic() - t0) / 60
    print(f"\nFinished in {mins:.1f} min — "
          f"ok={counts['ok']} empty={counts['empty']} "
          f"blocked={counts['blocked']} errors={counts['error']}")
    print(f"{new_count} stories added, {total} published.", flush=True)

    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
