import requests, json, os, ran
from bs4 import BeautifulSoup
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from huggingface_hub import HfApi, hf_hub_download
from collections import deque

HF_TOKEN = os.getenv("HF_TOKEN")
SHARD_ID = int(os.getenv("SHARD_ID", "1"))
HF_REPO = "HarshaAtmon10/Crawler"

def get_world_seeds():
    sites = []
    try:
        r = requests.get("https://tranco-list.eu/top-1m.csv", timeout=20)
        for line in r.text.split("\n")[:5000]:
            if "," in line:
                domain = line.split(",")[1].strip()
                if "." in domain and len(domain) < 30:
                    sites.append(f"https://{domain}")
        random.shuffle(sites)
        s = (SHARD_ID-1)*600
        return sites[s:s+600]
    except:
        return [f"https://en.wikipedia.org/wiki/Special:Random" for _ in range(100)]

SEED_URLS = get_world_seeds()
visited = set()
queue = deque(SEED_URLS)
collected = []
headers = {"User-Agent": f"ATMON-Bot/{SHARD_ID}.0 (World Crawler)"}
MAX = 2500

def can_crawl(url):
    try:
        p = urlparse(url)
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
        rp.read()
        return rp.can_fetch("ATMON-Bot", url)
    except:
        return True

count = 0
while queue and count < MAX:
    url = queue.popleft()
    if url in visited or len(url) > 200: continue
    visited.add(url)
    if not can_crawl(url): continue
    try:
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code!= 200: continue
        ctype = r.headers.get("Content-Type","")
        if "text/html" not in ctype and "xml" not in ctype: continue
        soup = BeautifulSoup(r.text, "html.parser")
        if "sitemap" in url or url.endswith(".xml"):
            for loc in soup.find_all("loc"):
                l = loc.text.strip().split("?")[0]
                if l not in visited and l.startswith("http"):
                    queue.append(l)
            continue
        title = soup.title.string.strip() if soup.title and soup.title.string else urlparse(url).netloc
        text = soup.get_text(" ", strip=True)[:1000]
        if len(text) > 150:
            collected.append({"title": title[:200], "url": url, "snippet": text, "domain": urlparse(url).netloc, "shard": SHARD_ID})
            count += 1
            print(f"[{SHARD_ID}] {count}: {urlparse(url).netloc}")
        for a in soup.find_all("a", href=True)[:80]:
            full = urljoin(url, a["href"]).split("#")[0].split("?")[0]
            if full.startswith("http") and full not in visited and len(queue) < 80000 and len(full) < 200:
                queue.append(full)
        if count % 100 == 0:
            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            queue.append(f"{base}/sitemap.xml")
    except:
        continue

existing = []
if HF_TOKEN:
    try:
        path = hf_hub_download(repo_id=HF_REPO, filename="metadata.json", repo_type="dataset", token=HF_TOKEN)
        with open(path, "r", encoding="utf-8") as f: existing = json.load(f)
    except: existing = []

combined = existing + collected
seen = set()
final_data = []
for item in reversed(combined):
    if item['url'] not in seen:
        seen.add(item['url'])
        final_data.append(item)
final_data = list(reversed(final_data))[-100000:]

with open("metadata.json","w",encoding="utf-8") as f:
    json.dump(final_data,f,ensure_ascii=False,indent=2)

if HF_TOKEN:
    HfApi().upload_file(path_or_fileobj="metadata.json", path_in_repo="metadata.json", repo_id=HF_REPO, repo_type="dataset", token=HF_TOKEN)
