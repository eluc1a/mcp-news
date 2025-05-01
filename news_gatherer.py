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
from feed_sources.feed_sources import FEED_SOURCES

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
# silence trafilatura's debug noise
logging.getLogger("trafilatura").setLevel(logging.ERROR)


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
            summarized_at TIMESTAMPTZ,
            uploaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
        INSERT INTO entries (id,title,link,published,source,category,content,summarized_at,uploaded_at)
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
                allow_redirects=True          # <— here's the new argument
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
                datetime.now(timezone.utc),
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