name: ATMON Full Real Crawler
on:
  workflow_dispatch:
  schedule:
    - cron: '0 */6 * * *'

jobs:
  crawl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install
        run: pip install datasets huggingface_hub pyarrow requests beautifulsoup4 lxml --quiet

      - name: ATMON Crawl
        run: |
          python << 'PYEOF'
          import requests, os, re, time, random
          from datetime import datetime
          from datasets import Dataset
          from huggingface_hub import HfApi

          HEADERS = {"User-Agent": "ATMON-Crawler/1.0 (Research; +https://huggingface.co/datasets/HarshaAtmon10/Crawler)"}
          all_data = []

          def clean(text):
              text = re.sub(r'\s+', ' ', text)
              text = text.strip()
              return text

          print("=== ATMON REAL CRAWLER START ===")

          # 1. TELUGU WIKIPEDIA - 400 Articles
          print("1. Telugu Wikipedia...")
          try:
              for _ in range(8): # 8*50 = 400
                  try:
                      r = requests.get("https://te.wikipedia.org/w/api.php?action=query&list=random&rnlimit=50&rnnamespace=0&format=json", headers=HEADERS, timeout=20)
                      r.raise_for_status()
                      pages = r.json()['query']['random']
                      for item in pages:
                          title = item['title']
                          try:
                              url = f"https://te.wikipedia.org/w/api.php?action=query&prop=extracts&explaintext&titles={title}&format=json"
                              pr = requests.get(url, headers=HEADERS, timeout=20).json()
                              pg = list(pr['query']['pages'].values())[0]
                              txt = pg.get('extract','')
                              if len(txt) > 400:
                                  all_data.append({
                                      "text": clean(txt[:8000]),
                                      "source": "te.wikipedia.org",
                                      "url": f"https://te.wikipedia.org/wiki/{title}",
                                      "lang": "te",
                                      "title": title
                                  })
                          except: continue
                  except: continue
                  time.sleep(0.8)
              print(f" -> TE Wiki: {len(all_data)}")
          except Exception as e:
              print(f"TE Wiki Fail: {e}")

          # 2. TELUGU WIKISOURCE - Books
          print("2. Telugu Wikisource...")
          try:
              for _ in range(2):
                  r = requests.get("https://te.wikisource.org/w/api.php?action=query&list=random&rnlimit=30&rnnamespace=0&format=json", headers=HEADERS, timeout=20)
                  pages = r.json()['query']['random']
                  for item in pages:
                      title = item['title']
                      try:
                          url = f"https://te.wikisource.org/w/api.php?action=query&prop=extracts&explaintext&titles={title}&format=json"
                          pr = requests.get(url, headers=HEADERS, timeout=20).json()
                          pg = list(pr['query']['pages'].values())[0]
                          txt = pg.get('extract','')
                          if len(txt) > 400:
                              all_data.append({
                                  "text": clean(txt[:8000]),
                                  "source": "te.wikisource.org",
                                  "url": f"https://te.wikisource.org/wiki/{title}",
                                  "lang": "te",
                                  "title": title
                              })
                      except: continue
                  time.sleep(0.8)
              print(f" -> After Wikisource: {len(all_data)}")
          except Exception as e:
              print(f"Wikisource Fail: {e}")

          # 3. ENGLISH WIKI - For Knowledge Mix (100)
          print("3. English Wikipedia...")
          try:
              for _ in range(2):
                  r = requests.get("https://en.wikipedia.org/w/api.php?action=query&list=random&rnlimit=50&rnnamespace=0&format=json", headers=HEADERS, timeout=20)
                  pages = r.json()['query']['random']
                  for item in pages:
                      title = item['title']
                      try:
                          url = f"https://en.wikipedia.org/w/api.php?action=query&prop=extracts&explaintext&titles={title}&format=json"
                          pr = requests.get(url, headers=HEADERS, timeout=20).json()
                          pg = list(pr['query']['pages'].values())[0]
                          txt = pg.get('extract','')
                          if len(txt) > 400:
                              all_data.append({
                                  "text": clean(txt[:5000]),
                                  "source": "en.wikipedia.org",
                                  "url": f"https://en.wikipedia.org/wiki/{title}",
                                  "lang": "en",
                                  "title": title
                              })
                      except: continue
                  time.sleep(0.8)
              print(f" -> After EN Wiki: {len(all_data)}")
          except Exception as e:
              print(f"EN Fail: {e}")

          # 4. FINAL CLEAN + DEDUP
          print(f"Total Raw: {len(all_data)}")
          seen = set()
          final = []
          for d in all_data:
              key = d['text'][:150]
              if key not in seen and len(d['text']) > 300:
                  seen.add(key)
                  d['timestamp'] = str(datetime.now())
                  d['crawl_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
                  final.append(d)

          random.shuffle(final)
          print(f"Total Final Unique: {len(final)}")

          if len(final) < 10:
              print("Too less data, aborting upload")
              exit(0)

          # 5. UPLOAD TO HF
          ds = Dataset.from_list(final)
          ts = datetime.now().strftime('%Y%m%d_%H%M%S')
          fname = f'atmon_data_{ts}.parquet'
          ds.to_parquet(fname)
          print(f"Saved {fname}: {len(ds)} rows, {os.path.getsize(fname)/1024:.1f} KB")

          api = HfApi()
          api.upload_file(
              path_or_fileobj=fname,
              path_in_repo=f'data/{fname}',
              repo_id='HarshaAtmon10/Crawler',
              repo_type='dataset',
              token=os.environ['HF_TOKEN']
          )
          print("=== SUCCESS - UPLOADED TO HF ===")
          print(f"Dataset: https://huggingface.co/datasets/HarshaAtmon10/Crawler")
          PYEOF
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
