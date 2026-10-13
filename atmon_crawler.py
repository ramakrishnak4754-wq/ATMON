import requests, json, os
from bs4 import BeautifulSoup
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from huggingface_hub import HfApi
from collections import deque

HF_TOKEN = os.getenv("HF_TOKEN")

# SEED - PRAPANCHAM MOTTHAM - ANNI LANGUAGES
SEED_URLS = [
    "https://en.wikipedia.org/wiki/World_Wide_Web",
    "https://te.wikipedia.org/wiki/తెలుగు_భాష",
    "https://hi.wikipedia.org/wiki/हिन्दी",
    "https://en.wikipedia.org/wiki/India",
    "https://www.bbc.com",
]

visited = set()
queue = deque(SEED_URLS)
collected = []

def can_crawl(url):
    """ALLOW RULE - Google laga"""
    try:
        p = urlparse(url)
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
        rp.read()
        return rp.can_fetch("ATMON-Bot", url)
    except:
        return True

headers = {"User-Agent":"ATMON-Bot/1.0 (ATMON Search; +https://atmon.in)"}

MAX = 500 # Okka run lo 500 sites - Prapancham mottham
count = 0

while queue and count < MAX:
    url = queue.popleft()
    if url in visited:
        continue
    visited.add(url)

    if not can_crawl(url):
        print(f"SKIP {url}")
        continue

    try:
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code!= 200:
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else url
        snippet = soup.get_text(" ", strip=True)[:500]

        # NO LANGUAGE FILTER - ANNI LANGUAGES SAVE!
        if len(snippet) > 50: # Page lo content unte chalu
            lang = "te" if "తె" in snippet else "hi" if "ह" in snippet else "en"
            collected.append({
                "title": title,
                "url": url,
                "snippet": snippet,
                "lang": lang,
                "crawler_id": f"Crawler-{(count%5)+1}"
            })
            count += 1
            print(f"✅ {count}: {title[:40]} [{lang}]")

        # PRAPANCHAM AUTO DISCOVERY - ANNI LINKS
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a.get("href"))
            full = full.split("#")[0].split("?")[0]
            if full.startswith("http") and full not in visited and full not in queue and len(queue) < 10000:
                # ANNI LANGUAGES LINKS ADD
                if len(full) < 200: # Too long urls skip
                    queue.append(full)

    except Exception as e:
        continue

print(f"TOTAL: {len(collected)} pages - ALL LANGUAGES!")

with open("metadata.json","w",encoding="utf-8") as f:
    json.dump(collected,f,ensure_ascii=False,indent=2)

if HF_TOKEN:
    HfApi().upload_file(path_or_fileobj="metadata.json", path_in_repo="metadata.json", repo_id="HarshaAtmon10/Crawler", repo_type="dataset", token=HF_TOKEN)
    print("PUSHED TO HF - ALL LANGUAGES - PHONE 0%")
