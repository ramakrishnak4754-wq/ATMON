# ATMON - ALL IN ONE - Google + ChatGPT Style Crawler
# ONE FILE = Website Opener + Robots.txt Check + JSON Save + Workflow Creator
import json, logging, time, os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup

# ===== CONFIG - NEE 10 SITES =====
WEBSITES = [
    "https://www.rbi.org.in/",
    "https://www.npci.org.in/",
    "https://www.isro.gov.in/",
    "https://en.wikipedia.org/wiki/Automated_teller_machine",
    "https://www.python.org",
    "https://www.github.com/",
    "https://www.nasa.gov/",
    "https://www.bbc.com/",
    "https://www.thehindu.com",
    "https://timesofindia.indiatimes.com"
]

OUTPUT_FILE = Path("data/crawl_results.json")
USER_AGENT = "ATMON-Bot/1.0"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ATMON")

def can_fetch(url):
    try:
        p = urlparse(url)
        robots = f"{p.scheme}://{p.netloc}/robots.txt"
        r = requests.get(robots, headers={"User-Agent": USER_AGENT}, timeout=15)
        if r.status_code == 404: return True
        if r.status_code in (401,403): return False
        parser = RobotFileParser()
        parser.parse(r.text.splitlines())
        return parser.can_fetch(USER_AGENT, url)
    except: return False

def crawl_one(session, url):
    res = {"url":url,"crawled_at":datetime.now(timezone.utc).isoformat(),"title":None,"content":None,"status_code":None,"error":None}
    if not can_fetch(url):
        res["error"]="Robots.txt disallows"; return res
    try:
        resp = session.get(url, headers={"User-Agent":USER_AGENT}, timeout=20)
        res["status_code"]=resp.status_code
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content,"html.parser")
        res["title"]=soup.title.string.strip()[:100] if soup.title else None
        for e in soup.select("script,style,noscript,nav,footer,header"): e.decompose()
        main = soup.find("main") or soup.find("article") or soup
        res["content"]=main.get_text(" ",strip=True)[:50000]
        print(f"✅ OPENED LIKE GOOGLE: {url} -> {res['title']}")
    except Exception as e:
        res["error"]=str(e)
        print(f"❌ FAILED: {url}")
    return res

def main():
    print(f"=== ATMON GOOGLE CRAWL START {datetime.now()} ===")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    results=[]
    with requests.Session() as s:
        for i,url in enumerate(WEBSITES):
            results.append(crawl_one(s,url))
            if i < len(WEBSITES)-1: time.sleep(2)

    ok = sum(1 for r in results if r["content"])
    final = {"crawler":"ATMON - Google Style","generated_at":datetime.now(timezone.utc).isoformat(),"total":len(WEBSITES),"success":ok,"failed":len(results)-ok,"results":results}

    with open(OUTPUT_FILE,"w",encoding="utf-8") as f:
        json.dump(final,f,indent=2,ensure_ascii=False)
    
    # Workflow kuda auto create
    Path(".github/workflows").mkdir(parents=True, exist_ok=True)
    with open(".github/workflows/crawl.yml","w") as f:
        f.write("""name: ATMON Auto Crawl
on:
  schedule:
    - cron: "30 1 * * *"
    - cron: "30 13 * * *"
  workflow_dispatch:
permissions:
  contents: write
jobs:
  crawl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install requests beautifulsoup4
      - run: python crawler.py
      - run: |
          git config user.name "ATMON-Bot"
          git config user.email "bot@atmon.com"
          git add data/crawl_results.json
          git commit -m "ATMON auto crawl" || true
          git push
""")
    print(f"\n🎉 DONE! {ok}/{len(WEBSITES)} sites opened like Google")
    print(f"📁 JSON saved: {OUTPUT_FILE}")
    print(f"🤖 Auto Workflow created: .github/workflows/crawl.yml")

if __name__=="__main__":
    main()
