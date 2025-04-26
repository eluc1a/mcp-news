#!/usr/bin/env python
"""
news_gatherer.py
Polls the configured RSS/Atom feeds, extracts full-text articles and stores
them in Postgres.  De-duplicates on (id) and ignores items older than N hours
(default: 6).  Logs per-source counts for fetched vs. inserted articles.
"""

import os
import uuid
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

import feedparser
import trafilatura
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─────────────────────────  configuration  ────────────────────────── #
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# look-back window (in hours) for skipping older items
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "6"))

# your Postgres URL
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/mcp_news")

# configure INFO-level logging with timestamps
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    level=logging.INFO
)
# silence trafilatura’s debug noise
logging.getLogger("trafilatura").setLevel(logging.ERROR)


# ────────────────────────  feed sources  ────────────────────────── #
FEED_SOURCES: List[Dict[str, str]] = [
    {"name": "TechCrunch",                "url": "https://techcrunch.com/feed/",                          "category": "tech"},
    {"name": "The Verge",                 "url": "https://www.theverge.com/rss/tech/index.xml",          "category": "tech"},
    {"name": "Ars Technica",              "url": "http://feeds.arstechnica.com/arstechnica/index",       "category": "tech"},
    {"name": "KDnuggets",                 "url": "https://www.kdnuggets.com/feed",                       "category": "data_science"},
    {"name": "Data Science Central",      "url": "https://www.datasciencecentral.com/feed/",             "category": "data_science"},
    {"name": "Machine Learning Mastery",  "url": "https://machinelearningmastery.com/feed/",            "category": "data_science"},
    {"name": "arXiv cs.LG",               "url": "https://arxiv.org/rss/cs.LG",                          "category": "data_science"},
    {"name": "arXiv stat.ML",             "url": "https://arxiv.org/rss/stat.ML",                        "category": "data_science"},
    {"name": "OpenAI News",               "url": "https://openai.com/news/rss.xml",                      "category": "llm_tools"},
    {"name": "Hugging Face Blog",         "url": "https://huggingface.co/blog/rss.xml",                  "category": "llm_tools"},
    {"name": "Google AI Blog",            "url": "https://ai.googleblog.com/atom.xml",                   "category": "llm_tools"},
    {"name": "The Hacker News",           "url": "https://feeds.feedburner.com/TheHackersNews",          "category": "cybersecurity"},
    {"name": "Krebs on Security",         "url": "https://krebsonsecurity.com/feed/",                    "category": "cybersecurity"},
    {"name": "Schneier on Security",      "url": "https://www.schneier.com/blog/atom.xml",               "category": "cybersecurity"},
    {"name": "Phoronix",                  "url": "https://www.phoronix.com/rss.php",                     "category": "linux"},
    {"name": "LWN.net",                   "url": "https://lwn.net/headlines/rss",                        "category": "linux"},
    {"name": "ServeTheHome",              "url": "https://www.servethehome.com/feed/",                   "category": "linux"},
    {"name": "The Audio Programmer",      "url": "https://theaudioprogrammer.com/rss/",                  "category": "audio_dsp"},
    {"name": "Designing Sound",           "url": "https://designingsound.org/feed/",                     "category": "audio_dsp"},
    {"name": "KVR Audio",                 "url": "https://www.kvraudio.com/xml/rss.xml",                 "category": "audio_dsp"},
    {"name": "VentureBeat",               "url": "https://venturebeat.com/feed/",                        "category": "startups"},
    {"name": "First Round Review",        "url": "https://review.firstround.com/rss",                    "category": "startups"},
    # General news sources
    {"name": "Reuters",                   "url": "https://feeds.reuters.com/reuters/topNews",            "category": "news"},
    {"name": "Associated Press",          "url": "https://apnews.com/apf-topnews?format=xml",            "category": "news"},
    {"name": "NPR",                       "url": "https://www.npr.org/rss/rss.php?id=1001",             "category": "news"},
    {"name": "PBS NewsHour",              "url": "https://www.pbs.org/newshour/rss.xml",                "category": "news"},
    {"name": "CBS News",                  "url": "https://www.cbsnews.com/latest/rss/main",             "category": "news"},
    {"name": "The New York Times",        "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "category": "news"},
    {"name": "The Washington Post",       "url": "https://feeds.washingtonpost.com/rss/homepage",       "category": "news"},
    {"name": "The Wall Street Journal",   "url": "https://www.wsj.com/xml/rss/3_7014.xml",               "category": "news"},
    {"name": "BBC News",                  "url": "http://feeds.bbci.co.uk/news/rss.xml",                "category": "news"},
    {"name": "The Guardian",              "url": "https://www.theguardian.com/world/rss",               "category": "news"},
    {"name": "Financial Times",           "url": "https://www.ft.com/?format=rss",                      "category": "news"},
    {"name": "Bloomberg",                 "url": "https://www.bloomberg.com/feed/podcast/etf-report.xml?via=rss", "category": "news"},
    {"name": "Al Jazeera English",        "url": "https://www.aljazeera.com/xml/rss/all.xml",            "category": "news"},
    {"name": "Deutsche Welle",            "url": "https://rss.dw.com/rdf/rss-en-all",                    "category": "news"},
    {"name": "NHK World – Japan",         "url": "https://www3.nhk.or.jp/rss/news/cat0.xml",             "category": "news"},
    {"name": "Christian Science Monitor", "url": "https://rss.csmonitor.com/feeds/all",                  "category": "news"},
    {"name": "ProPublica",                "url": "https://www.propublica.org/feeds/propublica.rss",      "category": "news"},
    {"name": "CBC News",                  "url": "http://rss.cbc.ca/lineup/topstories.xml",             "category": "news"},
    {"name": "South China Morning Post",  "url": "https://www.scmp.com/rss",                            "category": "news"},
    {"name": "Voice of America",          "url": "https://www.voanews.com/rssfeeds",                    "category": "news"},
]

# Additional reputable sources (20 entries)
FEED_SOURCES.extend([
    {"name": "Scientific American",             "url": "https://www.scientificamerican.com/feed/",                   "category": "science"},
    {"name": "Nature",                          "url": "https://www.nature.com/nature.rss",                         "category": "science"},
    {"name": "Science Magazine",                "url": "https://www.sciencemag.org/rss/news_current.xml",           "category": "science"},
    {"name": "PNAS",                            "url": "https://www.pnas.org/rss/current.xml",                      "category": "science"},
    {"name": "ScienceDaily",                    "url": "https://www.sciencedaily.com/rss/all.xml",                  "category": "science"},
    {"name": "New Scientist",                   "url": "https://www.newscientist.com/feed/home/",                  "category": "science"},
    {"name": "Phys.org",                        "url": "https://phys.org/rss-feed/",                                "category": "science"},
    {"name": "Live Science",                    "url": "https://www.livescience.com/feeds/all",                     "category": "science"},
    {"name": "The Scientist",                   "url": "https://www.the-scientist.com/feed/rss",                    "category": "science"},
    {"name": "EurekAlert!",                     "url": "https://www.eurekalert.org/rss.xml",                        "category": "science"},
    {"name": "Smithsonian (Science & Nature)",  "url": "https://www.smithsonianmag.com/rss/science-nature/",        "category": "science"},
    {"name": "IFLScience",                      "url": "https://www.iflscience.com/rss",                            "category": "science"},
    {"name": "Undark Magazine",                 "url": "https://undark.org/feed/",                                  "category": "science"},
    {"name": "Stories in Science",              "url": "https://storiesinscience.org/feed/",                        "category": "science"},
    {"name": "Starts With A Bang!",             "url": "https://medium.com/feed/starts-with-a-bang",                "category": "science"},
    {"name": "Pew Research Center",             "url": "https://www.pewresearch.org/rss/",                          "category": "research"},
    {"name": "Brookings Institution",           "url": "https://www.brookings.edu/feed/",                           "category": "policy"},
    {"name": "Council on Foreign Relations",    "url": "https://www.cfr.org/rss/all.xml",                           "category": "policy"},
    {"name": "RAND Corporation",                "url": "https://www.rand.org/rss.xml",                              "category": "policy"},
    {"name": "UN News",                         "url": "https://www.un.org/apps/news/rss/rss.xml",                  "category": "policy"},
])

FEED_SOURCES.extend([
    {"name": "C-SPAN",                                 "url": "https://www.c-span.org/rss/",                            "category": "news"},
     {"name": "The Economist (World News)", "url": "https://www.economist.com/international/rss.xml", "category": "news"},
    {"name": "The Economist (Science & Technology)", "url": "https://www.economist.com/science-and-technology/rss.xml", "category": "tech"},
    {"name": "The Economist (Business)", "url": "https://www.economist.com/business/rss.xml", "category": "business"},
    {"name": "The Economist (Finance & Economics)", "url": "https://www.economist.com/finance-and-economics/rss.xml", "category": "business"},
    {"name": "The Economist (Politics - Leaders)", "url": "https://www.economist.com/leaders/rss.xml", "category": "policy"},
    {"name": "The Economist (Climate Change)", "url": "https://www.economist.com/climate-change/rss.xml", "category": "science"},
    {"name": "The Economist (United States)", "url": "https://www.economist.com/united-states/rss.xml", "category": "news"},
    {"name": "The Economist (Technology Quarterly)", "url": "https://www.economist.com/technology-quarterly/rss.xml", "category": "tech"},
    {"name": "The Economist (The World Ahead)", "url": "https://www.economist.com/the-world-ahead/rss.xml", "category": "news"},
    {"name": "The Bureau of Investigative Journalism", "url": "https://www.thebureauinvestigates.com/feed/atom",        "category": "news"},
    {"name": "USA Today",                              "url": "https://rssfeeds.usatoday.com/UsatodaycomNation-TopStories", "category": "news"},
    {"name": "Forbes",                                 "url": "https://www.forbes.com/business/feed/",                "category": "news"},
    {"name": "UPI",                                    "url": "https://www.upi.com/rss/news/top_news/",                 "category": "news"},
    {"name": "Le Monde",                               "url": "https://www.lemonde.fr/en/rss/une.xml",                  "category": "news"},
    {"name": "Euronews",                               "url": "https://www.euronews.com/rss",                           "category": "news"},
    {"name": "Time Magazine",                          "url": "https://time.com/feed/",                                 "category": "news"}
])

FEED_SOURCES.extend([
    {"name": "We Want Science",              "url": "https://www.wewantscience.com/feed",                  "category": "science"},
    {"name": "WIRED Science",                "url": "https://www.wired.com/category/science/feed",         "category": "science"},
    {"name": "ZME Science",                  "url": "https://www.zmescience.com/feed/",                    "category": "science"},
    {"name": "Darknet Diaries",              "url": "https://feeds.megaphone.fm/darknetdiaries",                 "category": "cybersecurity"},
    {"name": "Graham Cluley",                "url": "https://grahamcluley.com/feed/",                      "category": "cybersecurity"},
    {"name": "SANS Internet Storm Center",   "url": "https://isc.sans.edu/rssfeed_full.xml",               "category": "cybersecurity"},
    {"name": "Securelist",                   "url": "https://securelist.com/feed/",                        "category": "cybersecurity"},
    {"name": "Troy Hunt",                    "url": "https://www.troyhunt.com/rss/",                       "category": "cybersecurity"},
    {"name": "WeLiveSecurity",               "url": "https://feeds.feedburner.com/eset/blog",              "category": "cybersecurity"},
    {"name": "Public Knowledge News",        "url": "https://publicknowledge.org/feed",                    "category": "policy"},
    {"name": "Dataconomy",                   "url": "https://dataconomy.com/feed",                         "category": "data_science"}
])

FEED_SOURCES.extend([
    {"name": "MacRumors",                      "url": "https://feeds.macrumors.com/MacRumors-All",       "category": "tech"},
    {"name": "9to5Mac",                        "url": "https://9to5mac.com/feed",                        "category": "tech"},
    {"name": "AppleInsider",                   "url": "https://appleinsider.com/rss/news",               "category": "tech"},
    {"name": "Gadgets Beat",                   "url": "https://gadgetsbeat.com/feed/",                   "category": "tech"},
    {"name": "Slashdot",                       "url": "http://rss.slashdot.org/Slashdot/slashdot",       "category": "tech"},
    {"name": "BAIR Blog",                      "url": "https://bair.berkeley.edu/blog/feed.xml",         "category": "data_science"},
    {"name": "Towards Data Science",           "url": "https://towardsdatascience.com/feed",             "category": "data_science"},
    {"name": "DeepMind Blog",                  "url": "https://deepmind.com/blog/feed/basic/",           "category": "llm_tools"},
    {"name": "LinuxJournal",                   "url": "https://www.linuxjournal.com/node/feed",          "category": "linux"},
    {"name": "LinuxToday",                     "url": "https://www.linuxtoday.com/feed/",                "category": "linux"},
    {"name": "LinuxInsider",                   "url": "https://linuxinsider.com/feed/",                  "category": "linux"},
    {"name": "HowToForge",                     "url": "https://www.howtoforge.com/feed.rss",             "category": "linux"},
    {"name": "Rocky Linux RSS",                "url": "https://rockylinux.org/rss.xml",                  "category": "linux"},
    {"name": "Latest Hacking News",            "url": "https://latesthackingnews.com/feed/",             "category": "cybersecurity"},
    {"name": "KitPloit",                       "url": "https://www.kitploit.com/feeds/posts/default",    "category": "cybersecurity"},
    {"name": "Microsoft Security Blog",        "url": "https://api.msrc.microsoft.com/update-guide/rss", "category": "cybersecurity"},
    {"name": "Fox Business",                   "url": "https://moxie.foxbusiness.com/google-publisher/latest.xml", "category": "business_tech"}
])

# ────────────────────────  database helpers  ──────────────────────── #
def get_connection():
    return psycopg2.connect(DB_URL, connect_timeout=5)

def init_db() -> None:
    """Ensure the entries table exists."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id            TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            link          TEXT NOT NULL,
            published     TIMESTAMPTZ,
            source        TEXT,
            category      TEXT,
            content       TEXT,
            summarized_at TIMESTAMPTZ
        );""")
        conn.commit()


# ─────────────────────────  feed harvesting  ────────────────────────── #
def _extract_content(url: str) -> Optional[str]:
    """Download article and return cleaned text via trafilatura."""
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                )
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None
    except Exception as exc:
        logging.debug("Request failed for %s: %s", url, exc)
        return None

    text = trafilatura.extract(
        r.text, include_comments=False, include_tables=False, no_fallback=True
    )
    return text.strip() if text else None

def upsert_entries(rows: List[tuple]) -> None:
    """Batch-insert new rows, ignoring conflicts on id."""
    if not rows:
        return
    query = """
        INSERT INTO entries (id,title,link,published,source,category,content,summarized_at)
        VALUES %s
        ON CONFLICT (id) DO NOTHING;"""
    with get_connection() as conn, conn.cursor() as cur:
        execute_values(cur, query, rows)
        conn.commit()


def fetch_and_store() -> int:
    """
    Harvest all configured feeds:
      • skip entries seen in the last LOOKBACK_HOURS
      • skip entries older than LOOKBACK_HOURS
    Returns total number of newly inserted articles.
    """
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    # preload IDs to avoid per-item DB lookups
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM entries WHERE published IS NULL OR published >= %s;",
            (cutoff_dt,),
        )
        recent_ids = {row[0] for row in cur.fetchall()}

    total_new = 0

    for src in FEED_SOURCES:
            # fetch via requests so we can follow redirects
        try:
            resp = requests.get(
                src["url"],
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
                allow_redirects=True          # <— here’s the new argument
            )
            if resp.status_code != 200:
                logging.warning("[%s] HTTP %s fetching %s",
                                src["name"], resp.status_code, src["url"])
                entries = []
            else:
                feed = feedparser.parse(resp.content)
                if feed.bozo:
                    logging.warning("[%s] parse error: %s",
                                    src["name"], feed.bozo_exception)
                entries = feed.entries or []
        except Exception as exc:
            logging.warning("[%s] request failed: %s", src["name"], exc)
            entries = []
        entries = feed.entries or []
        source_new = 0
        new_rows: List[tuple] = []

        for entry in entries:
            entry_id = (
                entry.get("id")
                or entry.get("guid")
                or entry.get("link")
                or str(uuid.uuid5(uuid.NAMESPACE_URL, entry.get("link", str(uuid.uuid4()))))
            )

            # skip duplicates within window
            if entry_id in recent_ids:
                continue

            # parse publish date
            pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            pub_dt = datetime(*pub_struct[:6], tzinfo=timezone.utc) if pub_struct else None

            # skip stale items
            if pub_dt and pub_dt < cutoff_dt:
                continue

            # extract or fallback to summary/description
            content = (
                _extract_content(entry.link)
                or entry.get("summary")
                or entry.get("description")
                or ""
            )
            if not content:
                continue

            new_rows.append((
                entry_id,
                entry.title,
                entry.link,
                pub_dt,
                src["name"],
                src["category"],
                content,
                None,
            ))
            recent_ids.add(entry_id)
            source_new += 1

        # insert this source's batch and log the counts
        upsert_entries(new_rows)
        total_new += source_new
        logging.info(
            "[%s] checked %d articles, inserted %d new",
            src["name"], len(entries), source_new
        )

    return total_new


# ───────────────────────  CLI entry-point  ────────────────────────── #
if __name__ == "__main__":
    init_db()
    inserted = fetch_and_store()
    print(
        f"[news_gatherer] inserted {inserted} new articles "
        f"(look-back {LOOKBACK_HOURS} h)."
    )