import requests, json, os, random, sys
from bs4 import BeautifulSoup
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from collections import deque

HF_TOKEN = os.getenv("HF_TOKEN")
SHARD_ID = int(os.getenv("SHARD_ID", "1"))
HF_REPO = "HarshaAtmon10/Crawler"

def get_world_seeds():
    sites = []
    try:
        r = requests.get("https://tranco-list.eu/top-1m.csv", timeout=30)
        for line in r.text.split("\n")[:5000]:
            parts = line.split(",")
            if len(parts) >= 2:
                domain = parts[1].strip()
                if "." in domain and len(domain) < 30 and " " not in domain:
                    sites.append(f"https://{domain}")
        random.shuffle(sites)
        s = (SHARD_ID-1)*600
        return sites[s:s+600]
    except:
        return ["https://en.wikipedia.org","https://www.bbc.com","https://www.github.com"]*100

SEED_URLS = get_world_seeds()
visited = set()
queue = deque(SEED_URLS)
collected = []
headers = {"User-Agent": f"ATMON-Bot/{SHARD_ID}.0"}
MAX = 2500

def can_crawl(url):
    try:
        p = urlparse(url)
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
        rp.read()
        return rp.can_fetch("*", url)
    except:
        return True

count = 0
while queue and count < MAX:
    url = queue.popleft()
    if url in visited or len(url) > 250: continue
    visited.add(url)
    if not can_crawl(url): continue
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code!= 200: continue
        if "text/html" not in r.headers.get("Content-Type",""): continue
        soup = BeautifulSoup(r.text, "html.parser")
        if "sitemap" in url or url.endswith(".xml"):
            for loc in soup.find_all("loc"):
                l = loc.text.strip().split("?")[0]
                if l not in visited and l.startswith("http"): queue.append(l)
            continue
        title = soup.title.string.strip() if soup.title and soup.title.string else urlparse(url).netloc
        text = soup.get_text(" ", strip=True)[:1000]
        if len(text) > 150:
            collected.append({"title": title[:200], "url": url, "snippet": text, "domain": urlparse(url).netloc, "shard": SHARD_ID})
            count += 1
        for a in soup.find_all("a", href=True)[:60]:
            full = urljoin(url, a["href"]).split("#")[0].split("?")[0]
            if full.startswith("http") and full not in visited and len(full) < 250: queue.append(full)
    except: continue

print(f"Collected {len(collected)}")
existing = []
if HF_TOKEN:
    try:
        from huggingface_hub import HfApi, hf_hub_download
        path = hf_hub_download(repo_id=HF_REPO, filename="metadata.json", repo_type="dataset", token=HF_TOKEN)
        with open(path, "r", encoding="utf-8") as f: existing = json.load(f)
    except: existing = []

final_data = list({x['url']: x for x in (existing + collected)[-100000:]}.values())
with open("metadata.json","w",encoding="utf-8") as f: json.dump(final_data,f,ensure_ascii=False,indent=2)
if HF_TOKEN and collected:
    try:
        from huggingface_hub import HfApi
        HfApi().upload_file(path_or_fileobj="metadata.json", path_in_repo="metadata.json", repo_id=HF_REPO, repo_type="dataset", token=HF_TOKEN)
    except: pass
