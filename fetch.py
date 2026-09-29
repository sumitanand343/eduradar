"""
fetch.py — EduRadar

Reads sources.yaml, fetches every RSS/Atom feed in parallel, applies
strict quality and relevance filters, classifies stories into 12 streams,
and writes:
  - data/stories.json
  - data/last_run.txt
  - data/stories.db

Quality rules:
  - The TITLE must contain an education/skills keyword (not just the summary)
  - Funding keywords are isolated — they never trigger other streams
  - A blocklist removes known low-quality sources
  - AI/tech/general-news sources pass an additional education gate on the title

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

# ── Streams & regions ─────────────────────────────────────────────────────────
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

# ── Domain blocklist ──────────────────────────────────────────────────────────
# Sources that produced too much noise or off-topic content.
# Add a domain here to silently drop ALL stories from that source.
BLOCKED_DOMAINS = {
    "developmentpathways.co.uk",   # general development economics, too broad
    "opportunitydesk.org",         # personal scholarships, not org-level resources
    "batimes.com.ar",              # general Argentine newspaper
    "thedailystar.net",            # general Bangladeshi newspaper
    "kathmandupost.com",           # general Nepali newspaper
    "ft.lk",                       # general Sri Lankan newspaper
    "japantimes.co.jp",            # general Japan newspaper
    "koreaherald.com",             # general Korean newspaper
    "scmp.com",                    # too broad for education
    "ideas.repec.org",             # general economics working papers
}

# ── Title-level education gate ────────────────────────────────────────────────
# The TITLE (not just the summary) must contain at least one of these.
# This prevents general news articles that happen to mention education
# in passing from passing the filter.
TITLE_EDUCATION_KEYWORDS = [
    # Core education terms
    "education", "educational", "school", "schooling", "student", "pupil",
    "teacher", "teaching", "classroom", "curriculum", "literacy", "numeracy",
    "learning", "learner", "tutor", "tutoring", "textbook", "lecture",
    "university", "college", "campus", "degree", "dropout", "enrollment",
    "enrolment", "exam", "examination", "kindergarten", "preschool",
    "early childhood", "k-12", "edtech", "ed-tech",
    # Skills and workforce
    "skill", "upskilling", "reskilling", "vocational", "tvet", "apprenticeship",
    "workforce", "labour market", "labor market", "future of work", "job market",
    "employment", "unemployment", "human capital", "digital skills",
    # AI in education specifically
    "ai in education", "ai in schools", "ai in learning", "ai tutor",
    "ai literacy", "artificial intelligence in education",
    "generative ai", "chatgpt", "edtech",
    # Research terms
    "learning outcome", "learning loss", "learning poverty",
    "early grade", "foundational", "egra", "egma", "tarl",
    # French / Spanish / Portuguese
    "éducation", "école", "educación", "educação", "escuela", "universidad",
]
_TITLE_RE = re.compile(
    r"(?<!\w)(?:" + "|".join(re.escape(w) for w in sorted(TITLE_EDUCATION_KEYWORDS, key=len, reverse=True)) + r")(?:s|es)?(?!\w)",
    re.IGNORECASE,
)

# ── Gated sources (general AI/tech/news) ─────────────────────────────────────
# These sources are education-gated meaning the TITLE must pass _TITLE_RE.
# All other sources only need to pass the domain blocklist check.
EDUCATION_GATED_SOURCES = {
    "arXiv — cs.CY (Computers & Society)",
    "OpenAI — News & Research",
    "Anthropic — News & Research",
    "Google — The Keyword Blog",
    "Google DeepMind — Blog",
    "MIT Technology Review — AI",
    "Stanford HAI — Human-Centered AI",
    "Allen Institute for AI (AI2)",
    "Partnership on AI — Blog",
    "AI Now Institute",
    "BBC — Business & Work",
    "The Guardian — Technology",
    "New York Times — Technology",
    "Al Jazeera — News (education-gated)",
    "WEF — Forum Stories",
    "WEF — Agenda",
    "NBER — Working Papers",
    "African Arguments — Development",
    "Dawn — Pakistan",
    "Nation Africa — Kenya",
    "Jeune Afrique — Societe",
    "Jakarta Post — Indonesia",
    "SciDev.Net — Sub-Saharan Africa",
    "SciDev.Net — Middle East & North Africa",
    "SciDev.Net — South-East Asia & Pacific",
    "SciDev.Net — Latin America",
    "Buenos Aires Times — Argentina",
    "The Daily Star — Bangladesh",
    "The Kathmandu Post — Nepal",
    "ReliefWeb — Education Jobs",
    "Opportunity Desk — Scholarships & Fellowships",
    "World Bank — South Asia Blog",
    "World Bank — Africa Blog",
    "World Bank — MENA Blog",
    "World Bank — Latin America Blog",
    "ILO — Working Papers",
    "UNDP — Blog",
    "UNICEF — Data for Action Blog",
}


def is_gated(source: dict) -> bool:
    return bool(source.get("gate")) or source.get("name") in EDUCATION_GATED_SOURCES


def passes_title_gate(title: str) -> bool:
    """Returns True if the title contains an education/skills keyword."""
    return bool(_TITLE_RE.search(title))


def is_blocked_domain(url: str) -> bool:
    domain = urlparse(url).netloc.lower().lstrip("www.")
    return any(domain == b or domain.endswith("." + b) for b in BLOCKED_DOMAINS)


# ── Quality filter ────────────────────────────────────────────────────────────
# Patterns that indicate off-topic or low-quality content.
# Any title matching these is dropped regardless of source.
NOISE_PATTERNS = re.compile(
    r"\b(lost money|made \$|crypto|bitcoin|forex|trading|casino|"
    r"dating|horoscope|celebrity|hollywood|bollywood|recipe|"
    r"fashion|beauty tip|weight loss|diet plan|net worth|"
    r"personal story|my journey|how I|student life at|"
    r"day in the life|what it's like to study)\b",
    re.IGNORECASE,
)


def passes_quality_filter(title: str) -> bool:
    return not NOISE_PATTERNS.search(title)


# ── Funding keywords — ISOLATED ───────────────────────────────────────────────
# These trigger the FUNDING stream ONLY. They are removed from all other
# stream classification so they never contaminate other streams.
FUNDING_KEYWORDS = re.compile(
    r"(?<!\w)(?:"
    r"grant|call for proposals|request for proposals|rfp|rfq|"
    r"job opening|job posting|job advertisement|vacancy|vacancies|"
    r"fellowship|internship opportunity|apply now|applications open|"
    r"deadline to apply|tender|funding opportunity|funded programme|"
    r"scholarship opportunity|award opportunity"
    r")(?:s|es)?(?!\w)",
    re.IGNORECASE,
)

# ── Stream classification ─────────────────────────────────────────────────────
# Rules are checked IN ORDER — first match wins.
# Funding is checked FIRST and is the only stream these keywords can go to.
# Each rule uses whole-word matching to avoid false positives.

def _re(words):
    alts = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(r"(?<!\w)(?:" + alts + r")(?:s|es)?(?!\w)", re.IGNORECASE)


STREAM_RULES = [
    # ── Funding — isolated, checked first ──────────────────────────────────
    ("funding", _re([
        "scholarship opportunity", "fellowship opportunity", "grant application",
        "call for proposals", "request for proposals", "rfp", "rfq",
        "job opening", "job posting", "vacancy", "vacancies",
        "apply now", "applications open", "deadline to apply",
        "tender", "funding opportunity", "funded programme",
    ])),

    # ── Research & Evidence ─────────────────────────────────────────────────
    ("research", _re([
        "randomized controlled", "randomised controlled", "rct",
        "impact evaluation", "impact study", "working paper",
        "meta-analysis", "systematic review", "preprint",
        "experimental study", "quasi-experimental",
        "replication study", "endline survey", "baseline survey",
        "learning assessment results", "egra", "egma",
        "evidence review", "new study finds", "study finds",
        "research finds", "research shows",
    ])),

    # ── Foundational Learning ───────────────────────────────────────────────
    # Strictly: reading and numeracy skills in early grades
    ("fln", _re([
        "foundational literacy", "foundational numeracy", "foundational learning",
        "foundational skills", "fln", "early grade reading", "early grade math",
        "early grade numeracy", "teaching at the right level", "tarl",
        "reading level", "learning poverty", "egra", "egma",
        "basic literacy", "basic numeracy",
    ])),

    # ── Early Childhood ─────────────────────────────────────────────────────
    # Strictly: ages 0-6, pre-primary, child development
    ("ece", _re([
        "early childhood education", "early childhood development",
        "early childhood care", "pre-primary", "preschool", "pre-school",
        "kindergarten", "nursery school", "playgroup",
        "ecd", "ece", "0-6", "under six", "toddler",
        "infant development", "child development",
        "early years education", "early learning centres",
    ])),

    # ── TVET & Vocational ───────────────────────────────────────────────────
    ("tvet", _re([
        "tvet", "technical and vocational", "vocational education",
        "vocational training", "vocational school",
        "apprenticeship programme", "apprenticeship scheme",
        "technical education", "trade school",
        "skills framework", "national qualifications framework",
        "competency-based training", "community college",
        "polytechnic", "vet system", "workforce training programme",
    ])),

    # ── Skills & Labor Market ───────────────────────────────────────────────
    ("labor", _re([
        "labor market", "labour market", "future of work",
        "skills demand", "skills shortage", "skills mismatch",
        "job displacement", "automation of jobs", "workforce skills",
        "human capital development", "skills gap",
        "reskilling programme", "upskilling programme",
        "digital skills training", "green skills",
        "jobs report", "employment report", "youth employment",
        "school-to-work", "work-based learning",
    ])),

    # ── Teacher Development ─────────────────────────────────────────────────
    ("teacher", _re([
        "teacher training", "teacher education", "teacher professional development",
        "teaching quality", "teacher workforce", "teacher shortage",
        "teacher recruitment", "in-service training", "pre-service training",
        "teacher support", "instructional coaching",
        "pedagogical", "teaching practice",
        "teacher assessment", "teacher evaluation",
    ])),

    # ── EdTech & AI Tools ───────────────────────────────────────────────────
    ("edtech", _re([
        "edtech", "ed-tech", "ai in education", "ai in learning",
        "ai in schools", "ai for education", "ai for learning",
        "artificial intelligence in education",
        "ai tutor", "ai tutoring", "intelligent tutoring system",
        "ai literacy", "personalized learning platform",
        "adaptive learning system", "learning analytics platform",
        "educational technology", "digital learning platform",
        "e-learning platform", "online learning platform",
        "learning management system", "lms platform",
        "khanmigo", "duolingo", "chatgpt in education",
        "generative ai in education", "large language model education",
    ])),

    # ── Secondary Education ─────────────────────────────────────────────────
    ("secondary", _re([
        "secondary school", "secondary education",
        "high school", "senior secondary", "upper secondary", "lower secondary",
        "middle school education", "a-level", "gcse",
        "baccalaureate", "secondary curriculum",
        "secondary school students", "secondary school teacher",
    ])),

    # ── Higher Education ────────────────────────────────────────────────────
    ("higher", _re([
        "higher education", "university education", "college education",
        "undergraduate programme", "postgraduate programme",
        "phd programme", "doctoral research",
        "university policy", "university funding", "university ranking",
        "academic freedom", "faculty", "campus",
        "higher education reform", "tuition fees",
        "student enrollment", "graduate programme",
    ])),

    # ── K-12 Education ──────────────────────────────────────────────────────
    # Broader school system — policy, operations, curriculum
    ("k12", _re([
        "primary school", "elementary school", "k-12", "k12",
        "basic education", "primary education",
        "school enrollment", "school enrolment",
        "out of school children", "school dropout",
        "school attendance", "school system",
        "school curriculum", "school policy",
        "school feeding programme", "girls education",
        "inclusive education", "special education needs",
        "school infrastructure", "school funding",
        "school leadership", "school management",
    ])),

    # ── Policy & Governance ─────────────────────────────────────────────────
    # Catch-all for education policy that doesn't fit above
    ("policy", _re([
        "education policy", "education reform", "education strategy",
        "education legislation", "ministry of education",
        "education governance", "education budget",
        "education regulation", "education accreditation",
        "national education plan", "education framework",
        "education system reform", "education spending",
    ])),
]


def classify(title: str, summary: str, hint: str) -> str:
    text = f"{title} {summary}"
    for stream, pattern in STREAM_RULES:
        if pattern.search(text):
            return stream
    # Fall back to source hint if no rule matches
    return hint if hint in STREAMS else "policy"


# ── robots.txt (5 second hard limit) ─────────────────────────────────────────
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


# ── Safe download with hard limits ───────────────────────────────────────────
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
                raise ValueError("feed larger than 5 MB")
            if time.monotonic() - start > TOTAL_TIMEOUT:
                raise TimeoutError(f"download took over {TOTAL_TIMEOUT}s")
        return b"".join(chunks)


# ── Parsing helpers ───────────────────────────────────────────────────────────
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
    """Returns (name, stories, status, message). Never raises."""
    name   = source.get("name", "Unnamed")
    url    = source.get("url", "")
    hint   = source.get("hint", "policy")
    region = source.get("region", "global")
    gated  = is_gated(source)

    # Check domain blocklist before even fetching
    if is_blocked_domain(url):
        return name, [], "blocked", "domain blocklist"

    try:
        if not can_fetch(url):
            return name, [], "blocked", "robots.txt disallows"

        feed = feedparser.parse(http_get(url))
        if not feed.entries:
            why = "not a valid RSS feed" if feed.bozo else "feed has no items"
            return name, [], "empty", why

        now_iso = datetime.now(timezone.utc).isoformat()
        stories, dropped_gate, dropped_quality, dropped_title = [], 0, 0, 0

        for entry in feed.entries:
            link    = entry.get("link")
            title   = clean(entry.get("title", ""), 300)
            summary = clean(entry.get("summary") or entry.get("description") or "", 600)

            if not link or not title:
                continue

            # 1. Quality filter — drop obvious noise regardless of source
            if not passes_quality_filter(title):
                dropped_quality += 1
                continue

            # 2. Title-level education gate:
            #    - Gated sources: title MUST contain an education keyword
            #    - All other sources: title must contain an education keyword too
            #      (prevents random personal blog posts slipping through)
            if not passes_title_gate(title):
                dropped_title += 1
                continue

            # 3. Additional gate for broad AI/news sources
            if gated and not passes_title_gate(title):
                dropped_gate += 1
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

        total_dropped = dropped_gate + dropped_quality + dropped_title
        msg = f"{len(stories)} kept"
        if total_dropped:
            msg += f", {total_dropped} dropped (title_gate={dropped_title} quality={dropped_quality})"
        return name, stories, "ok", msg

    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return name, [], "error", f"HTTP {code}"
    except Exception as exc:
        return name, [], "error", f"{type(exc).__name__}: {str(exc)[:90]}"


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
           (id, title, url, summary, source, stream, region, published, fetched_at)
           VALUES (:id,:title,:url,:summary,:source,:stream,:region,:published,:fetched_at)""",
        stories,
    )
    conn.commit()
    return conn.total_changes - before


def cutoff_iso() -> str:
    ts = datetime.now(timezone.utc).timestamp() - MAX_AGE_DAYS * 86400
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def prune_old(conn: sqlite3.Connection) -> None:
    # Also remove stories that were misclassified by old rules
    conn.execute("DELETE FROM stories WHERE published < ?", (cutoff_iso(),))
    conn.commit()


def clear_db(conn: sqlite3.Connection) -> None:
    """Wipe all stories so misclassified old content doesn't persist."""
    conn.execute("DELETE FROM stories")
    conn.commit()


# ── Export JSON ───────────────────────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> int:
    cutoff = cutoff_iso()
    result = {"streams": {}, "regions": {}}
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

    # Wipe old stories so misclassified content doesn't persist in the site
    print("Clearing old stories to apply new classification rules...", flush=True)
    clear_db(conn)

    print(f"Fetching {len(sources)} sources, {MAX_WORKERS} at a time...\n", flush=True)
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
        print(f"\n  [deadline] {len(sources) - done} slow sources skipped "
              f"after {FETCH_DEADLINE // 60} min", flush=True)
    pool.shutdown(wait=False, cancel_futures=True)

    prune_old(conn)
    total = build_json(conn)
    conn.close()

    mins = (time.monotonic() - t0) / 60
    print(f"\nFinished in {mins:.1f} min — "
          f"ok={counts['ok']} empty={counts['empty']} "
          f"blocked={counts['blocked']} errors={counts['error']}")
    print(f"{new_count} stories added, {total} published to the site.", flush=True)

    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
