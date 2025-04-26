#!/usr/bin/env python
"""
news_mcp_server.py
FastMCP server that exposes the harvested articles to LLM agents
and offers on-demand refreshing & summarisation.
"""

import os, logging
from datetime import datetime, timezone
from typing import List, Dict, Optional

import openai
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP, Context  # type: ignore

# Import DB helpers & harvester from the separate module
from news_gatherer import (
    get_connection,
    init_db,
    fetch_and_store,
)

# ─────────────────────────  configuration  ────────────────────────── #
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

SUMMARY_WORD_TARGET      = 500
MAX_ARTICLES_PER_SUMMARY = 25
KEYWORD_FILTER = [
    "tech","technology","data science","machine learning","ml",
    "foundation model","self-supervised","causal","llm",
    "large language model","prompt","agent","hack","cyber",
    "linux","open-source","homelab","audio","dsp","creative",
    "startup","entrepreneur","funding",
]

openai.api_key = os.getenv("OPENAI_API_KEY")

# ────────────────────────  summarisation  ─────────────────────────── #
def _summarize_articles(records: List[dict]) -> str:
    articles_md, sources = [], []
    for idx, rec in enumerate(records, 1):
        articles_md.append(
            f"### Article {idx}\n"
            f"Title: {rec['title']}\n"
            f"Source: {rec['source']} ({rec['link']})\n\n"
            f"{rec['content']}\n"
        )
        sources.append(f"[{idx}] {rec['title']} – {rec['link']}")
    articles_text = '\n'.join(articles_md)
    prompt = f"""
You are a professional technology analyst.
Write a cohesive {SUMMARY_WORD_TARGET}-word briefing combining the articles below,
grouping by theme. Cover only these domains: {', '.join(KEYWORD_FILTER)}.
Cite with footnotes like [1], [2]. Start with a short headline.

=== ARTICLES ===
{articles_text}
=== END ===
"""
    resp = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=3000,
    )
    summary = resp.choices[0].message.content.strip()
    summary += "\n\nSources\n" + "\n".join(sources)
    return summary

def summarize_unsummarized(category: Optional[str] = None,
                           limit: int = MAX_ARTICLES_PER_SUMMARY) -> str:
    with get_connection() as conn, conn.cursor() as cur:
        sql = """
        SELECT id,title,link,source,content
        FROM entries
        WHERE summarized_at IS NULL
        """
        params: list = []
        if category:
            sql += "AND category=%s"
            params.append(category)
        sql += "ORDER BY published DESC NULLS LAST LIMIT %s"
        params.append(limit)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    if not rows:
        return "No new articles to summarize."

    records = [dict(id=r[0], title=r[1], link=r[2], source=r[3], content=r[4])
               for r in rows]
    summary = _summarize_articles(records)
    ids = [r["id"] for r in records]

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE entries SET summarized_at=%s WHERE id=ANY(%s);",
                    (datetime.now(timezone.utc), ids))
        conn.commit()

    return summary

# ────────────────────────  Fast-MCP API  ──────────────────────────── #
mcp = FastMCP("News Feeds")

# @mcp.tool()
# def refresh_feeds(ctx: Context) -> str:
#     """Manually trigger the harvester (rarely needed if you schedule it)."""
#     inserted = fetch_and_store()
#     return f"Fetched feeds; inserted {inserted} new items."

@mcp.tool(
    annotations={
        "title": "Summarize News Articles",
        "readOnlyHint": True,
        "openWorldHint": False
    }
)
def summarize_news(ctx: Context, category: str = "", limit: int = 20) -> List[Dict]:
    """
        Returns raw articles so the caller can summarise them (LLM-side).
        
        Args:
            ctx: MCP context providing database connection and logging.
            category: Optional category filter. One of:
                tech, data_science, llm_tools, cybersecurity, linux,
                audio_dsp, startups, news, science, research, policy.
                Defaults to all categories.
            limit: Maximum number of articles to return (default 20,
                capped by MAX_ARTICLES_PER_SUMMARY).

        Returns:
            A list of dicts, each with keys:
                id (int), title (str), link (str),
                published (str), source (str), content (str).

        Available categories:
        "tech",
        "data_science",
        "llm_tools",
        "cybersecurity",
        "linux",
        "audio_dsp",
        "startups",
        "news",
        "science",
        "research",
        "policy",

        Examples: 
        # Generic Requests
        `summarize news`
        `news`

        # Time Requests
        `news 10` means get the last 10 articles
        `news 2 hours` means get the articles from the last 2 hours

        # Category Requests
        `news ai`
        `news machine learning`

        # Category & Time Requests
        `news 2 hours ai`
        `news machine learning 2 hours`
        `news 10 hours ai`
        `news 2 hours machine learning`

    """
    cat = category or None
    lim = min(limit, MAX_ARTICLES_PER_SUMMARY)
    with get_connection() as conn, conn.cursor() as cur:
        if cat:
            cur.execute("""
            SELECT id,title,link,published::text,source,content
            FROM entries
            WHERE category=%s
            ORDER BY published DESC NULLS LAST
            LIMIT %s;""", (cat, lim))
        else:
            cur.execute("""
            SELECT id,title,link,published::text,source,content
            FROM entries
            ORDER BY published DESC NULLS LAST
            LIMIT %s;""", (lim,))
        rows = cur.fetchall()

    return [
        dict(id=r[0], title=r[1], link=r[2], published=r[3],
             source=r[4], content=r[5])
        for r in rows
    ]

@mcp.resource("news://{category}/{limit}")
def get_latest_news(category: str, limit: int = 10) -> List[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        SELECT title,link,published::text,source
        FROM entries
        WHERE category=%s
        ORDER BY published DESC NULLS LAST
        LIMIT %s;""", (category, limit))
        return [
            dict(title=t, link=l, published=p, source=s)
            for t, l, p, s in cur.fetchall()
        ]

# ──────────────────────  server start-up  ─────────────────────────── #
def initialize_resources() -> None:
    """Ensure DB exists; harvesting is handled by the external scheduler."""
    init_db()
    logging.info("News MCP server initialised – waiting for client calls.")

if __name__ == "__main__":
    initialize_resources()
    # SSE transport keeps things simple for HTTP clients such as Cursor/TypingMind
    mcp.run(transport="stdio")