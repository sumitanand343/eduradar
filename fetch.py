"""
fetch.py — EduRadar

Reads sources.yaml, fetches every RSS/Atom feed in parallel, applies
a strict education gate to general media and AI lab sources, classifies
stories into multiple streams (up to 3 per story), and writes:
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
MAX_AGE_DAYS           = 90      # keep 90 days of history
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

# ── Streams ───────────────────────────────────────────────────────────────────
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
    "ideas.repec.org",           # general economics, too broad
}

# ── Education gate — ONLY for general media and AI lab sources ────────────────
# Education-specific orgs bypass this entirely.
EDUCATION_GATED_SOURCES = {
    "BBC — Business & Work",
    "The Guardian — Technology",
    "New York Times — Technology",
    "Al Jazeera — News (education-gated)",
    "Financial Times — Education",
    "The Economist — Education",
    "The Independent — Education",
    "Google — The Keyword Blog",
    "Google DeepMind — Blog",
    "OpenAI — News & Research",
    "Anthropic — News & Research",
    "MIT Technology Review — AI",
    "Allen Institute for AI (AI2)",
    "Partnership on AI — Blog",
    "AI Now Institute",
    "Stanford HAI — Human-Centered AI",
    "WEF — Forum Stories",
    "WEF — Agenda",
    "World Bank — News (education-gated)",
    "GIZ (Germany) — Development News",
    "JICA (Japan) — Press Releases",
    "Australian DFAT — Development",
    "Norad (Norway) — Aid Results",
    "Dawn — Pakistan",
    "African Arguments — Development",
    "NBER — Working Papers",
    "Rest of World",
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
    "generative ai", "chatgpt", "learning outcome", "learning loss",
    "learning poverty", "foundational", "egra", "egma", "tarl", "aser",
    "éducation", "école", "educación", "educação", "escuela",
]
_TITLE_RE = re.compile(
    r"(?<!\w)(?:" + "|".join(re.escape(w) for w in sorted(TITLE_EDU_KEYWORDS, key=len, reverse=True)) + r")(?:s|es)?(?!\w)",
    re.IGNORECASE,
)

# ── Opinion/noise filter ──────────────────────────────────────────────────────
# Blocks political opinion pieces and personal stories with no education substance
OPINION_NOISE_RE = re.compile(
    r"\b(advocates fear|under trump|opinion:|i think|my view|personal story|"
    r"my journey|how i|day in the life|what it's like|lost money|made \$|"
    r"crypto|bitcoin|casino|dating|horoscope|celebrity|recipe|fashion|"
    r"beauty tip|weight loss|net worth)\b",
    re.IGNORECASE,
)

# Signals that a story HAS real education substance (overrides opinion filter)
EDU_SUBSTANCE_RE = re.compile(
    r"\b(programme|program|policy|reform|research|study|data|report|"
    r"evaluation|funding|enrollment|curriculum|teacher|school|university|"
    r"student|learning|skills|tvet|vocational|edtech|ai in education)\b",
    re.IGNORECASE,
)


def passes_noise_filter(title: str, summary: str) -> bool:
    """Returns True if story should be kept."""
    if OPINION_NOISE_RE.search(title):
        # Only keep if it has real education substance
        return bool(EDU_SUBSTANCE_RE.search(title + " " + summary))
    return True


# ── Multi-stream classification ───────────────────────────────────────────────
# Each story can belong to up to 3 streams.
# Rules are checked against title + summary.
# Returns a list of matching stream keys (max 3).

def _re(words):
    alts = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(r"(?<!\w)(?:" + alts + r")(?:s|es)?(?!\w)", re.IGNORECASE)


# Each tuple: (stream_key, compiled_regex)
STREAM_RULES = [
    ("funding", _re([
        "scholarship opportunity", "fellowship opportunity", "grant application",
        "call for proposals", "rfp", "rfq", "job opening", "job posting",
        "vacancy", "vacancies", "apply now", "applications open",
        "deadline to apply", "tender", "funding opportunity",
    ])),
    ("research", _re([
        "randomized controlled", "randomised controlled", "rct",
        "impact evaluation", "working paper", "meta-analysis",
        "systematic review", "preprint", "quasi-experimental",
        "endline survey", "baseline survey", "egra", "egma", "aser",
        "evidence review", "study finds", "research finds", "new evidence",
        "learning assessment results",
    ])),
    ("foundational", _re([
        "foundational literacy", "foundational numeracy", "foundational learning",
        "foundational skills", "fln", "early grade reading", "early grade math",
        "teaching at the right level", "tarl", "reading level",
        "learning poverty", "basic literacy", "basic numeracy",
        "early childhood education", "early childhood development",
        "early childhood care", "pre-primary", "preschool", "pre-school",
        "kindergarten", "nursery school", "ecd", "ece",
        "child development", "early years education", "early learning",
        "play-based learning",
    ])),
    ("tvet", _re([
        "tvet", "technical and vocational", "vocational education",
        "vocational training", "vocational school", "apprenticeship",
        "technical education", "trade school", "skills framework",
        "national qualifications framework", "competency-based training",
        "community college", "polytechnic", "vet system",
        "workforce training programme",
    ])),
    ("labor", _re([
        "labor market", "labour market", "future of work",
        "skills demand", "skills shortage", "skills mismatch",
        "job displacement", "automation of jobs", "workforce skills",
        "human capital development", "skills gap", "reskilling",
        "upskilling", "digital skills training", "green skills",
        "jobs report", "employment report", "youth employment",
        "school-to-work", "workforce development",
    ])),
    ("teacher", _re([
        "teacher training", "teacher education",
        "teacher professional development", "teaching quality",
        "teacher workforce", "teacher shortage", "teacher recruitment",
        "in-service training", "pre-service training",
        "teacher support", "instructional coaching", "pedagogical",
        "teaching practice", "teacher assessment", "teacher evaluation",
        "classroom technology", "teachers and technology",
        "teachers use", "teachers weigh", "teachers report",
    ])),
    ("edtech", _re([
        "edtech", "ed-tech", "ai in education", "ai in learning",
        "ai in schools", "ai for education", "ai for learning",
        "artificial intelligence in education", "ai tutor", "ai tutoring",
        "intelligent tutoring system", "ai literacy",
        "personalized learning platform", "adaptive learning system",
        "learning analytics", "educational technology",
        "digital learning platform", "e-learning platform",
        "online learning platform", "learning management system",
        "khanmigo", "chatgpt in education", "generative ai in education",
        "large language model education", "ai and education",
        "technology in education", "digital education",
        "classroom technology", "education technology",
    ])),
    ("higher", _re([
        "higher education", "university education", "college education",
        "undergraduate programme", "postgraduate programme",
        "phd programme", "doctoral research", "university policy",
        "university funding", "university ranking", "academic freedom",
        "higher education reform", "tuition fees", "graduate programme",
        "faculty research",
    ])),
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
        "a-level", "gcse", "baccalaureate",
    ])),
    ("policy", _re([
        "education policy", "education reform", "education strategy",
        "education legislation", "ministry of education",
        "education governance", "education budget", "education spending",
        "education regulation", "education accreditation",
        "national education plan", "education framework",
        "education system reform", "skills policy",
    ])),
]

MAX_STREAMS_PER_STORY = 3


def classify_multi(title: str, summary: str, hint: str) -> list[str]:
    """Returns list of matching stream keys (up to MAX_STREAMS_PER_STORY)."""
    text    = f"{title} {summary}"
    matched = []
    for stream, pattern in STREAM_RULES:
        if pattern.search(text):
            matched.append(stream)
            if len(matched) >= MAX_STREAMS_PER_STORY:
                break
    if not matched:
        matched = [hint if hint in STREAMS else "policy"]
    return matched


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


# ── Safe download ─────────────────────────────────────────────────────────────
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


# ── Helpers ───────────────────────────────────────────────────────────────────
def clean(text: str, limit: int) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = html.unescape(text)
    return " ".join(text.split())[:limit]


def parse_date(entry) -> str:
    """Parse feed date, always return UTC ISO string. Never return future dates."""
    now = datetime.now(timezone.utc)
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        val = entry.get(attr)
        if val:
            try:
                # Use calendar.timegm to treat the parsed time as UTC
                ts = calendar.timegm(val)
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                # Cap at now — feeds sometimes have future-dated entries
                return min(dt, now).isoformat()
            except Exception:
                pass
    # No date in feed — use current time
    return now.isoformat()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


def is_blocked_domain(url: str) -> bool:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    return any(domain == b or domain.endswith("." + b) for b in BLOCKED_DOMAINS)


def is_gated(source: dict) -> bool:
    return bool(source.get("gate")) or source.get("name") in EDUCATION_GATED_SOURCES


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

            # Noise filter — drops opinion pieces with no education substance
            if not passes_noise_filter(title, summary):
                dropped += 1
                continue

            # Title gate for general media and AI lab sources
            if gated and not _TITLE_RE.search(title):
                dropped += 1
                continue

            streams = classify_multi(title, summary, hint)
            pub     = parse_date(entry)

            # Create one story entry per stream so it appears in each stream tab
            for stream in streams:
                sid = story_id(link + stream)   # unique per story+stream combo
                stories.append({
                    "id":         sid,
                    "title":      title,
                    "url":        link,
                    "summary":    summary,
                    "source":     name,
                    "stream":     stream,
                    "streams":    json.dumps(streams),  # all streams for badge display
                    "region":     region,
                    "published":  pub,
                    "fetched_at": now_iso,
                })

        msg = f"{len(set(s['url'] for s in stories))} stories, {sum(1 for s in stories) } stream entries"
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
            source TEXT, stream TEXT, streams TEXT, region TEXT,
            published TEXT, fetched_at TEXT
        )
    """)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(stories)")}
    for col, defval in [("region", "'global'"), ("streams", "NULL")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE stories ADD COLUMN {col} TEXT DEFAULT {defval}")
    conn.commit()


def save_stories(conn: sqlite3.Connection, stories: list) -> int:
    before = conn.total_changes
    conn.executemany(
        """INSERT OR IGNORE INTO stories
           (id,title,url,summary,source,stream,streams,region,published,fetched_at)
           VALUES (:id,:title,:url,:summary,:source,:stream,:streams,:region,:published,:fetched_at)""",
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
            """SELECT title, url, summary, source, published, region, streams
               FROM stories
               WHERE stream = ? AND published > ?
               ORDER BY published DESC LIMIT ?""",
            (key, cutoff, MAX_STORIES_PER_STREAM),
        ).fetchall()
        result["streams"][key] = [
            {"title": r[0], "url": r[1], "summary": r[2], "source": r[3],
             "published": r[4], "region": r[5],
             "streams": json.loads(r[6]) if r[6] else [key],
             "stream": key}
            for r in rows
        ]
        total += len(rows)

    for key in REGIONS:
        rows = conn.execute(
            """SELECT title, url, summary, source, published, stream, streams
               FROM stories
               WHERE region = ? AND published > ?
               GROUP BY url   -- deduplicate: one entry per URL per region
               ORDER BY published DESC LIMIT 40""",
            (key, cutoff),
        ).fetchall()
        result["regions"][key] = [
            {"title": r[0], "url": r[1], "summary": r[2], "source": r[3],
             "published": r[4], "stream": r[5],
             "streams": json.loads(r[6]) if r[6] else [r[5]],
             "region": key}
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

    print("Clearing old stories to apply new multi-stream structure...", flush=True)
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
    print(f"{new_count} stream entries added, {total} published.", flush=True)

    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
