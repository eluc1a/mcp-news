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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
mcp = FastMCP("News Feeds")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~  __    __     ______     ______  ~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~ /\ "-./  \   /\  ___\   /\  == \ ~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~ \ \ \-./\ \  \ \ \____  \ \  _-/ ~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~  \ \_\ \ \_\  \ \_____\  \ \_\   ~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~   \/_/  \/_/   \/_____/   \/_/   ~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@mcp.tool()
def summarize_news(ctx: Context, category: str | List[str] = "", hours: int = 24, limit: int = 10_000, offset: int = 0) -> Dict:
    """
        Returns raw articles so the caller can summarise them (LLM-side).
        
        Args:
            ctx: MCP context providing database connection and logging.
            category: Category filter - either a single category string or a list of categories.
                    Available categories include:
                    international_news, research, data_science, regional_international_news, 
                    business_finance_news, us_local_news, business_tech, tech, policy, linux, 
                    science, cybersecurity, startups, business, us_national_news, 
                    investigative_journalism, llm_tools
                Defaults to us_national_news if empty.
            hours: Number of hours to look back for articles.
            limit: Maximum number of articles to return.
            offset: Starting position for pagination (default 0).

        Returns:
            A dict with:
                - articles: list of article dicts (each has id, title, link, published, source, content)
                - meta: metadata about the query (total_count, limit, offset, has_more)
    """
    if isinstance(category, list):
        category_str = ', '.join(category)
        logger.info(f"summarize_news called with categories=[{category_str}], hours={hours}, limit={limit}, offset={offset}")
    else:
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