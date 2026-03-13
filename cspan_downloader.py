#!/usr/bin/env python3
"""
C-SPAN Video Downloader
-----------------------
Search and download C-SPAN videos by politician name.

Uses two search strategies:
  1. Direct C-SPAN search (scrapes c-span.org/search/)
  2. Google fallback (if C-SPAN blocks with WAF challenge)

Downloads are handled by yt-dlp which has native C-SPAN support.

Usage:
    python cspan_downloader.py "Nancy Pelosi"
    python cspan_downloader.py "Nancy Pelosi" --max 5 --output ./videos
    python cspan_downloader.py "Mitch McConnell" --list-only
    python cspan_downloader.py "Ted Cruz" --max 3 --format mp4
    python cspan_downloader.py --url "https://www.c-span.org/program/..." --output ./video
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import requests
from bs4 import BeautifulSoup

try:
    import yt_dlp
except ImportError:
    print("Error: yt-dlp is required. Install with: pip install yt-dlp")
    sys.exit(1)


CSPAN_BASE = "https://www.c-span.org"
CSPAN_SEARCH_URL = "https://www.c-span.org/search/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# C-SPAN URL patterns that yt-dlp can handle
CSPAN_VIDEO_PATTERN = re.compile(
    r"https?://(?:www\.)?c-span\.org/(?:program|video|clip)/[^\s\"'>]+"
)


# ---------------------------------------------------------------------------
# Search strategies
# ---------------------------------------------------------------------------

def _make_session():
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    return session


def _normalize_cspan_url(href):
    """Normalize a C-SPAN URL to a full https URL."""
    if not href:
        return None
    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        href = CSPAN_BASE + href
    if not href.startswith("http"):
        return None
    # Remove URL-encoded fragments and tracking params
    href = href.split("&")[0] if "&utm" in href else href
    return href


def search_cspan_direct(politician_name, max_results=10):
    """
    Search C-SPAN directly. Returns list of video dicts.
    May fail if C-SPAN's AWS WAF blocks the request.
    """
    session = _make_session()
    session.headers["Referer"] = CSPAN_BASE

    params = {"query": politician_name, "searchtype": "Videos"}

    for attempt in range(3):
        try:
            resp = session.get(CSPAN_SEARCH_URL, params=params, timeout=30)
            if resp.status_code == 200 and len(resp.text) > 1000 and "awsWaf" not in resp.text:
                break
        except requests.RequestException:
            pass
        time.sleep(2 * (attempt + 1))
    else:
        return None  # Signal that direct search failed (WAF or error)

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select("li.onevid")

    videos = []
    for item in items:
        if len(videos) >= max_results:
            break

        title_link = item.select_one("a.title")
        thumb_link = item.select_one("a.thumb")
        link = title_link or thumb_link
        if not link:
            continue

        url = _normalize_cspan_url(link.get("href", ""))
        if not url or ("/program/" not in url and "/video/" not in url and "/clip/" not in url):
            continue

        title = ""
        h3 = item.select_one("a.title h3")
        if h3:
            title = h3.get_text(strip=True)
        elif title_link:
            title = title_link.get_text(strip=True)

        date = ""
        time_el = item.select_one("time[datetime]")
        if time_el:
            date = time_el.get_text(strip=True)

        description = ""
        abstract = item.select_one("p.abstract")
        if abstract:
            description = abstract.get_text(strip=True)[:300]

        if url and title:
            videos.append({
                "title": title,
                "url": url,
                "date": date,
                "description": description,
            })

    return videos


def search_web_fallback(politician_name, max_results=10):
    """
    Use DuckDuckGo HTML search to find C-SPAN video URLs for a politician.
    This bypasses C-SPAN's WAF since we're querying a search engine, not C-SPAN.
    """
    query = f'site:c-span.org "{politician_name}" video'
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote_plus(query)}"

    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html",
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  Web search failed: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(html, "html.parser")
    videos = []
    seen_urls = set()

    # DuckDuckGo HTML results are in .result elements with .result__a links
    results = soup.select(".result__a")
    for a in results:
        href = a.get("href", "")

        # DuckDuckGo wraps URLs in uddg= parameter
        if "uddg=" in href:
            match = re.search(r"uddg=([^&]+)", href)
            if match:
                href = urllib.parse.unquote(match.group(1))

        # Check if it's a C-SPAN video/program/clip URL
        if not re.search(r"c-span\.org/(program|video|clip)/", href):
            continue

        # Skip person pages, search pages, etc.
        if "/person/" in href or "/search/" in href:
            continue

        # Deduplicate
        clean_url = href.split("&")[0].rstrip("/")
        if clean_url in seen_urls:
            continue
        seen_urls.add(clean_url)

        # Extract title from the link text
        title = a.get_text(strip=True)
        # Clean up " | Video | C-SPAN.org" suffix
        title = re.sub(r"\s*\|\s*(?:Video|C-SPAN\.org)\s*", "", title).strip(" |")

        if not title:
            slug_match = re.search(r"/(?:program|video|clip)/[^/]+/([^/]+)", clean_url)
            if slug_match:
                title = slug_match.group(1).replace("-", " ").title()

        if clean_url and title:
            videos.append({
                "title": title,
                "url": clean_url,
                "date": "",
                "description": "",
            })

        if len(videos) >= max_results:
            break

    return videos


def search_cspan_videos(politician_name, max_results=10):
    """
    Search for C-SPAN videos. Tries direct search first, falls back to Google.
    """
    # Try direct C-SPAN search
    videos = search_cspan_direct(politician_name, max_results)
    if videos is not None and len(videos) > 0:
        return videos

    if videos is None:
        print("  C-SPAN search blocked (WAF), falling back to Google search...")
    else:
        print("  No results from C-SPAN direct search, trying Google...")

    # Fallback to web search
    return search_web_fallback(politician_name, max_results)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def _resolve_cspan_url(video_url):
    """
    Resolve a C-SPAN URL to a format yt-dlp can handle.

    yt-dlp's CSpan extractor only supports /video/?ID URLs.
    For /program/ and /clip/ URLs, we try to:
      1. Fetch the page and find an m3u8 stream or /video/ link
      2. Construct the m3u8 URL from the numeric ID
      3. Fall back to the original URL for the generic extractor
    """
    # Already a supported /video/ URL
    if re.match(r"https?://(?:www\.)?c-span\.org/video/\?", video_url):
        return video_url

    # Extract numeric ID from /clip/ or /program/ URLs
    id_match = re.search(r"/(\d+)/?$", video_url)
    if not id_match:
        return video_url

    numeric_id = id_match.group(1)

    # Try to fetch the page and find the underlying video URL or m3u8
    session = _make_session()
    try:
        resp = session.get(video_url, timeout=30)
        if resp.status_code == 200 and len(resp.text) > 1000 and "awsWaf" not in resp.text:
            # Look for m3u8 stream URL
            m3u8_match = re.search(r'(https?://[^\s"]+\.m3u8[^\s"]*)', resp.text)
            if m3u8_match:
                return m3u8_match.group(1)
            # Look for /video/?ID links
            vid_match = re.search(r'c-span\.org/video/\?([a-f0-9]+)', resp.text)
            if vid_match:
                return f"https://www.c-span.org/video/?{vid_match.group(1)}"
    except Exception:
        pass

    # Try known m3u8 URL patterns. Program URLs use .tsc.m3u8, clips don't.
    is_clip = "/clip/" in video_url
    if is_clip:
        candidates = [
            f"https://m3u8-0.c-spanvideo.org/clip/clip.{numeric_id}.m3u8",
            f"https://m3u8-1.c-spanvideo.org/clip/clip.{numeric_id}.m3u8",
        ]
    else:
        candidates = [
            f"https://m3u8-0.c-spanvideo.org/program/program.{numeric_id}.tsc.m3u8",
            f"https://m3u8-1.c-spanvideo.org/program/program.{numeric_id}.tsc.m3u8",
        ]
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

    # Return original URL - yt-dlp generic extractor may still handle it
    return video_url


def download_video(video_url, output_dir=".", format_pref="mp4", quiet=False):
    """
    Download a single C-SPAN video using yt-dlp.
    Returns the path to the downloaded file, or None on failure.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Try to resolve to a yt-dlp-compatible URL
    resolved_url = _resolve_cspan_url(video_url)
    if resolved_url != video_url and not quiet:
        print(f"  Resolved to: {resolved_url}")

    outtmpl = os.path.join(output_dir, "%(upload_date>%Y-%m-%d,release_date>%Y-%m-%d,timestamp>%Y-%m-%d|Unknown Date)s - %(title).100s.%(ext)s")

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


def download_single_url(url, output_dir=".", format_pref="mp4", quiet=False):
    """Download a single C-SPAN URL directly (no search needed)."""
    os.makedirs(output_dir, exist_ok=True)
    print(f"Downloading: {url}")
    path = download_video(url, output_dir=output_dir, format_pref=format_pref, quiet=quiet)
    if path:
        print(f"Saved: {path}")
    else:
        print("Failed to download.")
    return path


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def search_and_download(
    politician_name,
    max_videos=5,
    output_dir=None,
    format_pref="mp4",
    list_only=False,
    quiet=False,
):
    """
    Main workflow: search for a politician's C-SPAN videos and download them.

    Args:
        politician_name: Name to search for (e.g. "Nancy Pelosi")
        max_videos: Maximum number of videos to download
        output_dir: Directory to save videos (default: ./cspan_{name}/)
        format_pref: Output format (default: mp4)
        list_only: If True, just list videos without downloading
        quiet: Suppress yt-dlp output

    Returns:
        List of dicts with video info and download paths
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
            desc = v["description"][:100]
            print(f"     {desc}")

    if list_only:
        return videos

    print(f"\nDownloading to: {output_dir}\n")
    os.makedirs(output_dir, exist_ok=True)

    # Save metadata
    meta_path = os.path.join(output_dir, "metadata.json")
    with open(meta_path, "w") as f:
        json.dump({"politician": politician_name, "videos": videos}, f, indent=2)

    results = []
    for i, v in enumerate(videos, 1):
        print(f"\n[{i}/{len(videos)}] Downloading: {v['title']}")
        path = download_video(
            v["url"], output_dir=output_dir, format_pref=format_pref, quiet=quiet
        )
        v["downloaded_path"] = path
        results.append(v)

        if path:
            print(f"  Saved: {path}")
        else:
            print("  Failed to download.")

        if i < len(videos):
            time.sleep(2)

    successful = sum(1 for r in results if r.get("downloaded_path"))
    print(f"\nDone! Downloaded {successful}/{len(results)} videos to {output_dir}")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Search and download C-SPAN videos by politician name.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "Nancy Pelosi"
  %(prog)s "Mitch McConnell" --max 5 --output ./mcconnell_videos
  %(prog)s "Ted Cruz" --list-only
  %(prog)s "Alexandria Ocasio-Cortez" --max 3 --format mp4 --quiet
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
        "--max", "-m", type=int, default=5,
        help="Max number of videos to download (default: 5)",
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

    args = parser.parse_args()

    # Direct URL download mode
    if args.url:
        output = args.output or "."
        path = download_single_url(
            args.url, output_dir=output, format_pref=args.format, quiet=args.quiet
        )
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
    )

    if not results:
        sys.exit(1)


if __name__ == "__main__":
    main()
