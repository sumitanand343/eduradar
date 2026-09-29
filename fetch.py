"""
fetch.py — EduRadar

Reads sources.yaml, fetches every RSS/Atom feed in parallel, keeps only
education-relevant stories from general AI/tech/news feeds, sorts each
story into one of 12 streams, and writes:
  - data/stories.json   (read by index.html)
  - data/last_run.txt   (timestamp shown on the site)
  - data/stories.db     (SQLite store, keeps history between runs)

Safety limits so a run can never hang:
  - robots.txt check: 5 second limit per site
  - each feed download: 20 seconds total, 5 MB max
  - whole fetch stage: stops waiting after 15 minutes and saves what it has

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

MAX_WORKERS     = 10          # feeds fetched at the same time
CONNECT_TIMEOUT = 5           # seconds to connect
READ_TIMEOUT    = 10          # seconds of silence before giving up
TOTAL_TIMEOUT   = 20          # seconds max for a whole feed download
MAX_BYTES       = 5_000_000   # 5 MB max per feed
ROBOTS_TIMEOUT  = 5           # seconds max for robots.txt
FETCH_DEADLINE  = 15 * 60     # stop waiting for slow feeds after 15 minutes

HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ROBOTS_AGENT = "Mozilla"   # robots.txt rules checked for this agent (generic "*" rules)

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

# ── Word matching helper ──────────────────────────────────────────────────────
# Whole words only (plus plural s/es), so "ece" does not match "recent",
# "stem" does not match "system", "grant" does not match "immigrant".
def compile_words(words):
    alts = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(r"(?<!\w)(?:" + alts + r")(?:s|es)?(?!\w)", re.IGNORECASE)

# ── Education gate ────────────────────────────────────────────────────────────
# These sources publish mostly general AI, tech or news content. A story from
# them is kept only if it is clearly about education, skills or work.
# A source can also be gated by adding "gate: true" to it in sources.yaml.
EDUCATION_GATED_SOURCES = {
    # AI labs and AI research
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
    # General media sections
    "BBC — Business & Work",
    "The Guardian — Technology",
    "New York Times — Technology",
    "Al Jazeera — News (education-gated)",
    "WEF — Technology & Innovation",
    "WEF — Global Risks & Geopolitics",
    # General science / development feeds
    "SciDev.Net — Education & Skills",
    "SciDev.Net — Sub-Saharan Africa",
    "SciDev.Net — Middle East & North Africa",
    "SciDev.Net — South-East Asia & Pacific",
    "SciDev.Net — Latin America",
    "NBER — Working Papers",
    "CGD — Center for Global Development",
    "ODI — Overseas Development Institute",
    "McKinsey Global Institute",
    "Open Society Foundations — News",
    "Plan International — News",
    "UN Women — News",
    "UNICEF — Education",
    "UNDP — Human Development",
    "African Arguments — Development",
    "World Bank — South Asia Blog",
    "World Bank — Africa Blog",
    "World Bank — MENA Blog",
    "World Bank — Latin America Blog",
    # Donor news feeds
    "JICA (Japan) — Press Releases",
    "KOICA (South Korea) — News",
    "Norad (Norway) — Aid Results",
    "Australian DFAT — Development",
    # General national newspapers
    "Dawn — Pakistan",
    "The Daily Star — Bangladesh",
    "The Kathmandu Post — Nepal",
    "Daily FT — Sri Lanka",
    "Nation Africa — Kenya",
    "The New Times — Rwanda",
    "Jeune Afrique — Societe",
    "Jakarta Post — Indonesia",
    "Buenos Aires Times — Argentina",
    # General jobs / opportunity boards
    "ReliefWeb — Jobs (Education)",
    "Opportunity Desk — Scholarships & Fellowships",
}

EDUCATION_GATE_KEYWORDS = [
    # English
    "education", "educational", "educator", "school", "schooling", "student",
    "pupil", "teacher", "teaching", "classroom", "curriculum", "pedagogy",
    "literacy", "numeracy", "learner", "learning outcome", "learning loss",
    "e-learning", "elearning", "online learning", "distance learning",
    "blended learning", "lifelong learning", "adult learning", "early learning",
    "university", "college", "campus", "faculty", "tuition", "enrolment",
    "enrollment", "dropout", "exam", "examination", "homework", "lecture",
    "lesson", "tutor", "tutoring", "textbook", "scholarship", "fellowship",
    "degree", "graduate", "k-12", "kindergarten", "preschool",
    "early childhood", "child development", "stem education",
    "skill", "skills gap", "upskilling", "reskilling", "digital skills",
    "vocational", "tvet", "apprenticeship", "apprentice", "internship",
    "workforce", "labour market", "labor market", "future of work",
    "jobs", "job market", "employment", "unemployment", "youth employment",
    "human capital", "edtech", "ed-tech", "ai tutor", "ai literacy",
    # French
    "éducation", "école", "enseignement", "enseignant", "élève",
    "étudiant", "formation professionnelle", "apprentissage",
    # Spanish / Portuguese
    "educación", "educação", "escuela", "escola", "docente", "maestro",
    "estudiante", "universidad", "aprendizaje", "alfabetización",
]
_GATE_RE = compile_words(EDUCATION_GATE_KEYWORDS)

# AI jargon that contains education words but is not about education.
# Removed before the gate check, so "machine learning" alone does not pass.
_AI_JARGON_RE = re.compile(
    r"\b(machine|deep|reinforcement|federated|transfer|representation|"
    r"self-supervised|semi-supervised|supervised|unsupervised|in-context|"
    r"contrastive|curriculum|few-shot|zero-shot|meta|continual|active|online)"
    r"[\s-]+learning\b|\blearning rates?\b|\btraining (data|runs?|compute|set)\b|"
    r"\bmodel training\b|\bpre-?training\b|\bfine-?tuning\b",
    re.IGNORECASE,
)


def is_gated(source: dict) -> bool:
    return bool(source.get("gate")) or source.get("name") in EDUCATION_GATED_SOURCES


def passes_education_gate(title: str, summary: str) -> bool:
    text = _AI_JARGON_RE.sub(" ", f"{title} {summary}")
    return bool(_GATE_RE.search(text))


# ── Stream classification — first matching stream wins ───────────────────────
KEYWORD_RULES = [
    ("funding", [
        "grant", "call for proposals", "request for proposals", "rfp", "rfq",
        "vacancy", "fellowship", "scholarship", "tender", "funding opportunity",
        "job opening", "apply now", "applications open", "deadline to apply",
    ]),
    ("research", [
        "randomized", "randomised", "rct", "evaluation", "working paper",
        "impact study", "meta-analysis", "systematic review", "preprint",
        "journal", "dissertation", "replication", "endline", "baseline survey",
        "learning assessment", "egra", "egma", "study finds", "new study",
    ]),
    ("fln", [
        "foundational literacy", "foundational numeracy", "foundational learning",
        "fln", "early grade reading", "early grade math",
        "teaching at the right level", "tarl", "numeracy", "reading skill",
        "basic literacy", "learning poverty",
    ]),
    ("ece", [
        "early childhood", "pre-primary", "preschool", "kindergarten", "ecd", "ece",
        "early learning", "nursery school", "child development", "early years",
    ]),
    ("tvet", [
        "tvet", "vocational", "apprenticeship", "technical education",
        "workforce training", "skills framework", "competency-based",
        "technical and vocational", "community college", "polytechnic",
        "trade training", "vet system",
    ]),
    ("labor", [
        "labor market", "labour market", "future of work", "skills demand",
        "job displacement", "automation", "employment", "unemployment", "wage",
        "human capital", "skills gap", "workforce development", "reskilling",
        "upskilling", "digital skills", "green skills", "jobs report",
    ]),
    ("teacher", [
        "teacher training", "teacher education", "professional development",
        "pedagogy", "teaching quality", "teacher workforce", "teacher shortage",
        "in-service training", "pre-service", "teacher support",
        "instructional coaching",
    ]),
    ("edtech", [
        "edtech", "ed-tech", "ai in education", "ai in learning", "ai in schools",
        "ai for education", "artificial intelligence in education", "ai tutor",
        "ai literacy", "personalized learning", "personalised learning",
        "adaptive learning", "learning analytics", "khan academy", "khanmigo",
        "duolingo", "digital learning", "e-learning", "elearning",
        "online learning", "learning platform", "lms", "intelligent tutoring",
        "chatgpt", "generative ai", "large language model", "artificial intelligence",
    ]),
    ("secondary", [
        "secondary school", "secondary education", "high school",
        "upper secondary", "lower secondary", "a-level", "gcse",
        "baccalaureate", "adolescent",
    ]),
    ("higher", [
        "university", "higher education", "college", "undergraduate",
        "postgraduate", "phd", "doctoral", "campus", "degree", "tuition",
    ]),
    ("k12", [
        "primary school", "elementary school", "k-12", "k12", "basic education",
        "primary education", "school enrolment", "school enrollment",
        "out of school", "out-of-school", "dropout", "school attendance",
    ]),
    ("policy", [
        "policy", "strategy", "regulation", "legislation", "ministry",
        "government", "reform", "national plan", "curriculum", "accreditation",
        "education system", "governance", "budget",
    ]),
]
_RULES_RE = [(stream, compile_words(words)) for stream, words in KEYWORD_RULES]


def classify(title: str, summary: str, hint: str) -> str:
    text = f"{title} {summary}"
    for stream, pattern in _RULES_RE:
        if pattern.search(text):
            return stream
    return hint if hint in STREAMS else "policy"


# ── robots.txt (5 second limit, never blocks other workers) ──────────────────
_robots_cache: dict = {}
_robots_lock = threading.Lock()


def _load_robots(base: str):
    """Returns a parser, or None meaning 'no rules, allowed'."""
    try:
        r = requests.get(f"{base}/robots.txt", headers=HEADERS,
                         timeout=(ROBOTS_TIMEOUT, ROBOTS_TIMEOUT))
    except Exception:
        return None
    if r.status_code >= 400:          # no robots.txt = no rules
        return None
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(r.text[:500_000].splitlines())
    return rp


def can_fetch(url: str) -> bool:
    p = urlparse(url)
    base = f"{p.scheme}://{p.netloc}"
    with _robots_lock:
        if base in _robots_cache:
            rp = _robots_cache[base]
            return True if rp is None else rp.can_fetch(ROBOTS_AGENT, url)
    rp = _load_robots(base)               # network call happens OUTSIDE the lock
    with _robots_lock:
        _robots_cache[base] = rp
    return True if rp is None else rp.can_fetch(ROBOTS_AGENT, url)


# ── Download with hard limits ─────────────────────────────────────────────────
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
                return min(dt, now).isoformat()   # feed dates are UTC; no future dates
            except Exception:
                pass
    return now.isoformat()


def story_id(url: str) -> str:
    return sha256(url.encode()).hexdigest()[:16]


# ── Fetch one feed ────────────────────────────────────────────────────────────
def fetch_feed(source: dict):
    """Returns (name, stories, status, message). Never raises."""
    name   = source.get("name", "Unnamed source")
    url    = source.get("url", "")
    hint   = source.get("hint", "policy")
    region = source.get("region", "global")
    gated  = is_gated(source)

    try:
        if not can_fetch(url):
            return name, [], "blocked", "robots.txt disallows"

        feed = feedparser.parse(http_get(url))
        if not feed.entries:
            why = "not a valid RSS feed" if feed.bozo else "feed has no items"
            return name, [], "empty", why

        now_iso, stories, dropped = datetime.now(timezone.utc).isoformat(), [], 0
        for entry in feed.entries:
            link    = entry.get("link")
            title   = clean(entry.get("title", ""), 300)
            summary = clean(entry.get("summary") or entry.get("description") or "", 600)
            if not link or not title:
                continue
            if gated and not passes_education_gate(title, summary):
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
            msg += f", {dropped} dropped by education gate"
        return name, stories, "ok", msg

    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return name, [], "error", f"HTTP {code}"
    except Exception as exc:
        return name, [], "error", f"{type(exc).__name__}: {str(exc)[:90]}"


# ── Database (only the main thread touches it) ────────────────────────────────
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
           VALUES (:id, :title, :url, :summary, :source, :stream, :region, :published, :fetched_at)""",
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


# ── Export JSON for the website ───────────────────────────────────────────────
def build_json(conn: sqlite3.Connection) -> int:
    cutoff = cutoff_iso()
    result = {"streams": {}, "regions": {}}
    total = 0

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

    # Load sources, skipping duplicate URLs
    raw = yaml.safe_load(SOURCES.read_text(encoding="utf-8"))["sources"]
    sources, seen = [], set()
    for s in raw:
        u = (s or {}).get("url")
        if u and u not in seen:
            seen.add(u)
            sources.append(s)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    print(f"Fetching {len(sources)} sources, {MAX_WORKERS} at a time...\n", flush=True)
    t0 = time.monotonic()
    counts = {"ok": 0, "empty": 0, "blocked": 0, "error": 0}
    new_count, done = 0, 0

    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
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
        print(f"\n  [deadline] {len(sources) - done} slow sources skipped after "
              f"{FETCH_DEADLINE // 60} minutes", flush=True)
    pool.shutdown(wait=False, cancel_futures=True)

    prune_old(conn)
    total = build_json(conn)
    conn.close()

    mins = (time.monotonic() - t0) / 60
    print(f"\nFinished in {mins:.1f} min. "
          f"ok={counts['ok']} empty={counts['empty']} "
          f"blocked={counts['blocked']} errors={counts['error']}")
    print(f"{new_count} new stories added, {total} stories published to the site.",
          flush=True)

    # Exit immediately so a stuck background download can't keep the job alive
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
