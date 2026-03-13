#!/usr/bin/env python3
"""
C-SPAN Video Downloader
-----------------------
Search and download C-SPAN videos by politician name.

Uses Playwright (headless browser) to bypass C-SPAN's WAF protection,
then yt-dlp to download videos.

Usage:
    python cspan_downloader.py "Eli Crane"
    python cspan_downloader.py "Nancy Pelosi" --max 10 --output ./videos
    python cspan_downloader.py "Mitch McConnell" --list-only
    python cspan_downloader.py --url "https://www.c-span.org/program/..." -o ./video

Requirements:
    pip install playwright yt-dlp beautifulsoup4 requests
    python -m playwright install chromium
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

try:
    import yt_dlp
except ImportError:
    print("Error: yt-dlp is required. Install with: pip install yt-dlp")
    sys.exit(1)

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Error: playwright is required. Install with:")
    print("  pip install playwright && python -m playwright install chromium")
    sys.exit(1)


CSPAN_BASE = "https://www.c-span.org"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Playwright browser helpers
# ---------------------------------------------------------------------------

def _launch_browser(playwright):
    """Launch a stealth Chromium browser."""
    browser = playwright.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"],
    )
    ctx = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
    )
    page = ctx.new_page()
    page.add_init_script(
        'Object.defineProperty(navigator, "webdriver", { get: () => undefined });'
    )
    return browser, page


def _wait_for_cspan(page, timeout=15):
    """Wait for C-SPAN page to load past the WAF challenge."""
    for _ in range(timeout):
        title = page.title()
        if title and "request could not" not in title.lower():
            count = page.locator("li.onevid").count()
            if count > 0:
                return True
        time.sleep(1)
    return False


def _normalize_url(href):
    """Normalize a C-SPAN URL to full https."""
    if not href:
        return None
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return CSPAN_BASE + href
    if href.startswith("http"):
        return href
    return None


# ---------------------------------------------------------------------------
# Search: find person page URL via DuckDuckGo
# ---------------------------------------------------------------------------

def _find_person_page(politician_name):
    """Use DuckDuckGo to find the politician's C-SPAN person page URL."""
    query = f'site:c-span.org/person {politician_name}'
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote_plus(query)}"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT, "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    soup = BeautifulSoup(html, "html.parser")
    name_lower = politician_name.lower()
    name_parts = name_lower.split()

    for a in soup.select(".result__a"):
        href = a.get("href", "")
        if "uddg=" in href:
            match = re.search(r"uddg=([^&]+)", href)
            if match:
                href = urllib.parse.unquote(match.group(1))
        if "c-span.org/person/" not in href:
            continue
        link_text = a.get_text(strip=True).lower()
        if all(part in link_text for part in name_parts):
            return href.rstrip("/") + "/"

    return None


def _extract_person_id(person_url):
    """Extract numeric person ID from a C-SPAN person URL."""
    match = re.search(r"/(\d+)/?$", person_url)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Search: Playwright-based scraping of C-SPAN search results
# ---------------------------------------------------------------------------

def _scrape_search_page(page):
    """Extract video info from the current Playwright page's li.onevid items."""
    videos = []
    items = page.locator("li.onevid").all()

    for item in items:
        title_el = item.locator("a.title h3").first
        time_el = item.locator("time").first
        link_el = item.locator("a.title").first
        abstract_el = item.locator("p.abstract").first

        title = title_el.text_content().strip() if title_el.count() else ""
        date = time_el.get_attribute("datetime") if time_el.count() else ""
        href = link_el.get_attribute("href") if link_el.count() else ""
        description = abstract_el.text_content().strip()[:300] if abstract_el.count() else ""

        url = _normalize_url(href)
        if url and title and ("/program/" in url or "/video/" in url or "/clip/" in url):
            videos.append({
                "title": title,
                "url": url,
                "date": date,
                "description": description,
            })

    return videos


def search_cspan_videos(politician_name, max_results=100):
    """
    Search C-SPAN for all videos featuring a politician.

    Strategy:
      1. Find the politician's C-SPAN person page via DuckDuckGo
      2. Use Playwright to browse the search filtered by person ID
      3. Paginate through all results (20 per page)
      4. Fall back to name-based search if person page not found
    """
    # Step 1: Find person page to get the person ID
    print("  Finding person page...")
    person_url = _find_person_page(politician_name)
    person_id = _extract_person_id(person_url) if person_url else None

    if person_id:
        print(f"  Found: {person_url}")
        search_url = (
            f"{CSPAN_BASE}/search/?searchtype=Videos&sort=Newest"
            f"&personid[]={person_id}"
        )
    else:
        print("  Person page not found, searching by name...")
        search_url = (
            f"{CSPAN_BASE}/search/?searchtype=Videos&sort=Newest"
            f"&query={urllib.parse.quote_plus(politician_name)}"
        )

    # Step 2: Use Playwright to scrape paginated results
    print("  Loading search results (browser)...")
    videos = []
    seen_urls = set()

    with sync_playwright() as pw:
        browser, page = _launch_browser(pw)

        try:
            pg = 1
            while len(videos) < max_results:
                url = search_url if pg == 1 else f"{search_url}&page={pg}"
                page.goto(url, wait_until="domcontentloaded", timeout=30000)

                if not _wait_for_cspan(page):
                    if pg == 1:
                        print("  Failed to load search results (WAF blocked).")
                        break
                    else:
                        break  # No more pages

                new_videos = _scrape_search_page(page)
                if not new_videos:
                    break

                new_count = 0
                for v in new_videos:
                    key = v["url"].rstrip("/")
                    if key not in seen_urls:
                        seen_urls.add(key)
                        videos.append(v)
                        new_count += 1

                print(f"  Page {pg}: {new_count} new videos (total: {len(videos)})")

                if new_count == 0 or len(videos) >= max_results:
                    break

                pg += 1
                time.sleep(2)

        finally:
            browser.close()

    return videos[:max_results]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _resolve_cspan_url(video_url):
    """
    Resolve a C-SPAN URL to a format yt-dlp can handle.

    yt-dlp's CSpan extractor supports /video/?ID URLs.
    For /program/ URLs, try the m3u8 stream URL.
    """
    if re.match(r"https?://(?:www\.)?c-span\.org/video/\?", video_url):
        return video_url

    id_match = re.search(r"/(\d+)/?$", video_url)
    if not id_match:
        return video_url

    numeric_id = id_match.group(1)

    # Try m3u8 URL patterns
    is_clip = "/clip/" in video_url
    if is_clip:
        candidates = [
            f"https://m3u8-0.c-spanvideo.org/clip/clip.{numeric_id}.m3u8",
        ]
    else:
        candidates = [
            f"https://m3u8-0.c-spanvideo.org/program/program.{numeric_id}.tsc.m3u8",
        ]

    session = requests.Session()
    for pattern in candidates:
        try:
            resp = session.get(
                pattern, timeout=10,
                headers={"Referer": CSPAN_BASE + "/", "User-Agent": USER_AGENT},
            )
            if resp.status_code == 200 and "#EXTM3U" in resp.text:
                return pattern
        except Exception:
            continue

    return video_url


def download_video(video_url, output_dir=".", format_pref="mp4", quiet=False):
    """
    Download a single C-SPAN video using yt-dlp.
    Returns the path to the downloaded file, or None on failure.
    """
    os.makedirs(output_dir, exist_ok=True)

    resolved_url = _resolve_cspan_url(video_url)
    if resolved_url != video_url and not quiet:
        print(f"  Resolved to: {resolved_url}")

    outtmpl = os.path.join(
        output_dir,
        "%(upload_date>%Y-%m-%d,release_date>%Y-%m-%d,timestamp>%Y-%m-%d|Unknown Date)s"
        " - %(title).100s.%(ext)s",
    )

    ydl_opts = {
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": format_pref,
        "outtmpl": outtmpl,
        "http_headers": {
            "User-Agent": USER_AGENT,
            "Referer": CSPAN_BASE + "/",
        },
        "quiet": quiet,
        "no_warnings": quiet,
        "ignoreerrors": False,
        "retries": 3,
        "fragment_retries": 5,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(resolved_url, download=True)
            if info:
                filename = ydl.prepare_filename(info)
                base, _ = os.path.splitext(filename)
                final = base + "." + format_pref
                if os.path.exists(final):
                    return final
                if os.path.exists(filename):
                    return filename
                return filename
    except Exception as e:
        print(f"  Error downloading {video_url}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def search_and_download(
    politician_name,
    max_videos=100,
    output_dir=None,
    format_pref="mp4",
    list_only=False,
    quiet=False,
    parallel=3,
):
    """
    Search for a politician's C-SPAN videos and download them.
    """
    if output_dir is None:
        output_dir = os.path.join(".", f"{politician_name} CSpan Videos")

    print(f"Searching C-SPAN for: {politician_name}")
    videos = search_cspan_videos(politician_name, max_results=max_videos)

    if not videos:
        print("No videos found. Try a different name or check your connection.")
        return []

    print(f"\nFound {len(videos)} video(s):\n")
    for i, v in enumerate(videos, 1):
        date_str = f" ({v['date']})" if v["date"] else ""
        print(f"  {i}. {v['title']}{date_str}")
        print(f"     {v['url']}")
        if v.get("description"):
            print(f"     {v['description'][:100]}")

    if list_only:
        return videos

    print(f"\nDownloading to: {output_dir}\n")
    os.makedirs(output_dir, exist_ok=True)

    meta_path = os.path.join(output_dir, "metadata.json")
    with open(meta_path, "w") as f:
        json.dump({"politician": politician_name, "videos": videos}, f, indent=2)

    def _download_one(idx_video):
        idx, v = idx_video
        print(f"\n[{idx}/{len(videos)}] Downloading: {v['title']}")
        path = download_video(
            v["url"], output_dir=output_dir, format_pref=format_pref, quiet=quiet,
        )
        v["downloaded_path"] = path
        if path:
            print(f"  [{idx}/{len(videos)}] Saved: {path}")
        else:
            print(f"  [{idx}/{len(videos)}] Failed to download.")
        return v

    workers = max(1, min(parallel, len(videos)))
    if workers == 1:
        results = [_download_one((i, v)) for i, v in enumerate(videos, 1)]
    else:
        print(f"  (downloading {workers} videos in parallel)\n")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_download_one, (i, v)) for i, v in enumerate(videos, 1)]
            results = [f.result() for f in futures]

    successful = sum(1 for r in results if r.get("downloaded_path"))
    print(f"\nDone! Downloaded {successful}/{len(results)} videos to {output_dir}")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Search and download C-SPAN videos by politician name.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "Eli Crane"
  %(prog)s "Nancy Pelosi" --max 10
  %(prog)s "Ted Cruz" --list-only
  %(prog)s "Mitch McConnell" --max 50 --output ./mcconnell --quiet
  %(prog)s --url "https://www.c-span.org/program/.../12345" -o ./single_video
        """,
    )
    parser.add_argument(
        "politician", nargs="?", default=None,
        help="Name of the politician to search for",
    )
    parser.add_argument(
        "--url", "-u", default=None,
        help="Download a specific C-SPAN video URL directly (skip search)",
    )
    parser.add_argument(
        "--max", "-m", type=int, default=100,
        help="Max number of videos (default: 100, i.e. all)",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output directory (default: ./{name} CSpan Videos/)",
    )
    parser.add_argument(
        "--format", "-f", default="mp4",
        help="Output format (default: mp4)",
    )
    parser.add_argument(
        "--list-only", "-l", action="store_true",
        help="List videos without downloading",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress yt-dlp download output",
    )
    parser.add_argument(
        "--parallel", "-p", type=int, default=1,
        help="Number of parallel downloads (default: 3)",
    )

    args = parser.parse_args()

    if args.url:
        output = args.output or "."
        os.makedirs(output, exist_ok=True)
        print(f"Downloading: {args.url}")
        path = download_video(
            args.url, output_dir=output, format_pref=args.format, quiet=args.quiet,
        )
        if path:
            print(f"Saved: {path}")
        else:
            print("Failed to download.")
        sys.exit(0 if path else 1)

    if not args.politician:
        parser.error("Please provide a politician name or use --url for a direct download.")

    results = search_and_download(
        politician_name=args.politician,
        max_videos=args.max,
        output_dir=args.output,
        format_pref=args.format,
        list_only=args.list_only,
        quiet=args.quiet,
        parallel=args.parallel,
    )

    if not results:
        sys.exit(1)


if __name__ == "__main__":
    main()
