#!/usr/bin/env python
"""
news_mcp_server.py
FastMCP server that exposes the harvested articles to LLM agents
and offers on-demand refreshing & summarisation.
"""

import os
import logging
import sys
from datetime import datetime, timezone, timedelta
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
MAX_ARTICLES_PER_SUMMARY = 10_000
MAX_ARTICLES_PER_RESPONSE = 10_000  # Maximum articles per API response due to transport layer limitations
KEYWORD_FILTER = [
    "tech","technology","data science","machine learning","ml",
    "foundation model","self-supervised","causal","llm",
    "large language model","prompt","agent","hack","cyber",
    "linux","open-source","homelab","audio","dsp","creative",
    "startup","entrepreneur","funding",
]

openai.api_key = os.getenv("OPENAI_API_KEY")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_mcp_server.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("news_mcp")

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

def summarize_unsummarized(
    category: Optional[str] = None,
    limit: int = MAX_ARTICLES_PER_SUMMARY
) -> str:
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

def get_articles_with_pagination(category: Optional[str], cutoff_time: datetime, limit: int) -> List[Dict]:
    """
    Get articles with pagination to support retrieving large numbers of articles.
    This function is internal and not exposed as an MCP tool.
    """
    print(f"*** RETRIEVING ARTICLES: category={category}, cutoff={cutoff_time}, limit={limit}")
    
    all_articles = []
    offset = 0
    batch_size = 100  # Each database query fetches 100 records
    
    with get_connection() as conn:
        while len(all_articles) < limit:
            with conn.cursor() as cur:
                if category:
                    query = """
                    SELECT id,title,link,published::text,source,content
                    FROM entries
                    WHERE category=%s AND uploaded_at >= %s
                    ORDER BY uploaded_at DESC NULLS LAST, published DESC NULLS LAST
                    LIMIT %s OFFSET %s;"""
                    
                    print(f"*** EXECUTING QUERY: OFFSET={offset}, LIMIT={batch_size}")
                    cur.execute(query, (category, cutoff_time, batch_size, offset))
                else:
                    query = """
                    SELECT id,title,link,published::text,source,content
                    FROM entries
                    WHERE category='news' AND uploaded_at >= %s
                    ORDER BY uploaded_at DESC NULLS LAST, published DESC NULLS LAST
                    LIMIT %s OFFSET %s;"""
                    
                    print(f"*** EXECUTING QUERY: OFFSET={offset}, LIMIT={batch_size}")
                    cur.execute(query, (cutoff_time, batch_size, offset))
                
                rows = cur.fetchall()
                print(f"*** FETCHED {len(rows)} ARTICLES")
                
                if not rows:
                    # No more results
                    break
                
                articles = [
                    dict(id=r[0], title=r[1], link=r[2], published=r[3],
                        source=r[4], content=r[5])
                    for r in rows
                ]
                
                all_articles.extend(articles)
                offset += batch_size
                
                if len(rows) < batch_size:
                    # Got fewer rows than requested, meaning we've reached the end
                    break
    
    print(f"*** TOTAL ARTICLES FOUND: {len(all_articles)}")
    return all_articles[:limit]  # Respect the original limit

@mcp.tool()
def summarize_news(ctx: Context, category: str = "", hours: int = 24, limit: int = 10_000, offset: int = 0) -> Dict:
    """
        Returns raw articles so the caller can summarise them (LLM-side).
        
        Args:
            ctx: MCP context providing database connection and logging.
            category: Optional category filter. One of:
                tech, data_science, llm_tools, cybersecurity, linux,
                audio_dsp, startups, news, science, research, policy.
                Defaults to all categories.
            hours: Number of hours to look back for articles.
            limit: Maximum number of articles to return.
            offset: Starting position for pagination (default 0).

        Returns:
            A dict with:
              - articles: list of article dicts (each has id, title, link, published, source, content)
              - meta: metadata about the query (total_count, limit, offset, has_more)
    """
    logger.info(f"summarize_news called with category='{category}', hours={hours}, limit={limit}, offset={offset}")
    
    current_time = datetime.now(timezone.utc)
    if hours >= 24:
        # Calculate how many days the hours represent
        days = hours // 24
        # Get the start of today, then subtract the days
        today_start = datetime(
            current_time.year, current_time.month, current_time.day, 
            tzinfo=current_time.tzinfo)
        cutoff_time = today_start - timedelta(days=days)
    else:
        # For less than 24 hours, use the original hour-based calculation
        cutoff_time = current_time - timedelta(hours=hours)
    
    logger.info(f"Calculated cutoff_time: {cutoff_time}")
    
    cat = category or None
    lim = min(limit, MAX_ARTICLES_PER_RESPONSE)  # Only fetch what we can actually return
    
    # Retrieve the articles directly
    articles = get_articles_with_pagination(cat, cutoff_time, lim)
    
    if not articles:
        # Return an empty result if no articles found
        return {
            "articles": [],
            "meta": {
                "total_count": 0,
                "limit": lim,
                "offset": offset,
                "has_more": False,
                "next_offset": None
            }
        }
    
    # Format all articles into a single text string with clear delimiters
    delimiter = "\n==========ARTICLE_SEPARATOR==========\n"
    
    result = f"FOUND {len(articles)} ARTICLES\n\n"
    result += delimiter.join([
        f"ID: {art['id']}\nTITLE: {art['title']}\nLINK: {art['link']}\n"
        f"PUBLISHED: {art['published']}\nSOURCE: {art['source']}\n"
        f"CONTENT:\n{art['content']}"
        for art in articles
    ])
    
    # Create a single "mega-article" with all content to bypass pagination limits
    mega_article = [{
        "id": 0,
        "title": f"News Feed ({len(articles)} articles)",
        "link": "concat://articles",
        "published": str(current_time),
        "source": "News Concatenator",
        "content": result
    }]
    
    # Return both the article and metadata
    return {
        "articles": mega_article,
        "meta": {
            "total_count": len(articles),
            "limit": lim,
            "offset": offset,
            "has_more": False,
            "next_offset": None
        }
    }

@mcp.resource("news://{category}/{limit}")
def get_latest_news(category: str, limit: int = 10_000) -> List[dict]:
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
    print("News MCP server initialised – waiting for client calls.")

if __name__ == "__main__":
    initialize_resources()
    # SSE transport keeps things simple for HTTP clients such as Cursor/TypingMind
    mcp.run(transport="stdio")