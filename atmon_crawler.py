import requests
import json
import os
from bs4 import BeautifulSoup
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from huggingface_hub import HfApi
from collections import deque

# Token from GitHub Secrets - Cloud Only
HF_TOKEN = os.getenv("HF_TOKEN")

# SEED URLS - Starting points for world-wide crawling
SEED_URLS = [
    "https://en.wikipedia.org/wiki/World_Wide_Web",
    "https://en.wikipedia.org/wiki/India",
    "https://te.wikipedia.org/wiki/Telugu_language",
    "https://hi.wikipedia.org/wiki/Hindi",
    "https://www.bbc.com",
]

visited = set()
queue = deque(SEED_URLS)
collected_data = []

def can_crawl(base_url, target_url):
    """Check robots.txt - Only crawl if allowed"""
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch("ATMON-Bot", target_url)
    except:
        return True

headers = {
    "User-Agent": "ATMON-Bot/1.0 (ATMON Search Engine; +https://atmon.in)"
}

MAX_PAGES = 500 # Pages per run
count = 0

print("ATMON 5 Mega Crawlers Started - Collecting All Languages Worldwide")

while queue and count < MAX_PAGES:
    current_url = queue.popleft()

    if current_url in visited:
        continue
    visited.add(current_url)

    if not can_crawl(current_url, current_url):
        print(f"SKIP - Disallowed by robots.txt: {current_url}")
        continue

    try:
        response = requests.get(current_url, headers=headers, timeout=12)
        if response.status_code!= 200:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else current_url
        snippet = soup.get_text(" ", strip=True)[:500]

        # Collect all languages - No filter
        if len(snippet) > 50:
            # Simple language detection
            if any("\u0c00" <= c <= "\u0c7f" for c in snippet):
                lang = "te"
            elif any("\u0900" <= c <= "\u097f" for c in snippet):
                lang = "hi"
            else:
                lang = "en"

            collected_data.append({
                "title": title,
                "url": current_url,
                "snippet": snippet,
                "language": lang,
                "crawler_id": f"Crawler-{(count % 5) + 1}"
            })
            count += 1
            print(f"[{count}] Saved: {title[:50]} [{lang}]")

        # Auto-discovery - Find new links - World-wide expansion
        for link_tag in soup.find_all("a", href=True):
            href = link_tag.get("href")
            full_url = urljoin(current_url, href)
            full_url = full_url.split("#")[0].split("?")[0]

            if (full_url.startswith("http") and
                full_url not in visited and
                full_url not in queue and
                len(queue) < 10000 and
                len(full_url) < 200):
                queue.append(full_url)

    except Exception as error:
        print(f"Error: {current_url} - {error}")
        continue

print(f"Total Collected: {len(collected_data)} pages from worldwide web")

# Save locally first
with open("metadata.json", "w", encoding="utf-8") as file:
    json.dump(collected_data, file, ensure_ascii=False, indent=2)

# Push directly to Hugging Face Dataset - Zero Phone Storage
if HF_TOKEN:
    api = HfApi()
    api.upload_file(
        path_or_fileobj="metadata.json",
        path_in_repo="metadata.json",
        repo_id="HarshaAtmon10/Crawler",
        repo_type="dataset",
        token=HF_TOKEN
    )
    print("Successfully pushed to Hugging Face - Cloud Storage - Phone 0%")
else:
    print("HF_TOKEN not found - Saved locally only")
