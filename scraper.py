# All-in-One Colab Cell: Lead Scraper → Google Sheets (with Instagram extraction)

# 1) Install & import dependencies

# 2) Authenticate in Colab
from google.colab import auth
auth.authenticate_user()

import gspread, google.auth, re
from gspread.exceptions import WorksheetNotFound, APIError
creds, _ = google.auth.default()
gc = gspread.authorize(creds)

import os, datetime, random, time, requests, pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

try:
    from googlesearch import search
    GOOGLE_OK = True
except ImportError:
    GOOGLE_OK = False

# 3) Interactive inputs (paste full Sheet URL)
sheet_url = input("📑 Paste Google Sheet URL: ").strip()
match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
if not match:
    raise ValueError("Invalid Google Sheet URL")
sheet_id = match.group(1)

city   = input("📍 City (e.g., Karachi, Chicago): ").strip().title()
target = int(input("🔢 How many unique leads?: ").strip())
mode   = input("🗺️ Source ('directories','google','both'): ").strip().lower()
if mode not in ("directories","google","both"):
    print("Invalid mode; defaulting to 'both'")
    mode = "both"

# 4) Open or create worksheet
sh = gc.open_by_key(sheet_id)
worksheets = sh.worksheets()
titles_lower = [ws.title.lower() for ws in worksheets]

if city.lower() in titles_lower:
    idx = titles_lower.index(city.lower())
    ws = worksheets[idx]
else:
    ws = sh.add_worksheet(title=city, rows="1000", cols="20")

HEADERS = ["Clinic Name", "City", "Website", "Email", "Phone", "Instagram", "Source URL", "Date Added"]
values = ws.get_all_values()
if not values:
    ws.append_row(HEADERS)
else:
    existing_headers = values[0]
    if existing_headers != HEADERS:
        ws.clear()
        ws.append_row(HEADERS)

# 5) Load existing leads into dedupe sets
vals = ws.get_all_values()
seen_websites = set()
seen_emails   = set()
if len(vals) > 1:
    hdr = vals[0]
    w_idx = hdr.index("Website")
    e_idx = hdr.index("Email")
    for row in vals[1:]:
        if w_idx < len(row) and row[w_idx]:
            seen_websites.add(row[w_idx])
        if e_idx < len(row) and row[e_idx]:
            seen_emails.add(row[e_idx])

# 6) Scraper config
MAX_RETRIES, TIMEOUT = 5, 10
DELAY_MIN, DELAY_MAX = 1, 3
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)..."
]
EMAIL_RE = re.compile(r"[\w.\-+]+@[\w.\-+]+\.[A-Za-z]+")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")

def get_ua():
    return random.choice(USER_AGENTS)

def create_session():
    s = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1,
                    status_forcelist=[429,500,502,503,504],
                    respect_retry_after_header=True)
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": get_ua()})
    return s

# 7) Scraper functions
def scrape_whatclinic(city, sess, today):
    url = f"https://www.whatclinic.com/dentists/{city.lower().replace(' ','-')}/pakistan"
    leads=[]
    try:
        r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for c in soup.select(".listing"):
            nm, lk = c.select_one("h2"), c.select_one("a[href]")
            leads.append({
                "Clinic Name": nm.text.strip() if nm else "N/A",
                "City": city,
                "Website": lk["href"] if lk else "N/A",
                "Email":"N/A","Phone":"N/A","Instagram":"N/A",
                "Source URL": url,"Date Added": today
            })
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    except:
        pass
    return leads

def scrape_yellowpages(city, sess, today):
    url = f"https://www.yellowpages.com/search?search_terms=dentist&geo_location_terms={city.replace(' ','+')}"
    leads=[]
    try:
        r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for c in soup.select(".result"):
            nm, ph = c.select_one("a.business-name"), c.select_one(".phones")
            leads.append({
                "Clinic Name": nm.text.strip() if nm else "N/A",
                "City": city,
                "Website": nm["href"] if nm and nm.has_attr("href") else "N/A",
                "Email":"N/A","Phone":ph.text.strip() if ph else "N/A",
                "Instagram":"N/A","Source URL": url,"Date Added": today
            })
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    except:
        pass
    return leads

def scrape_google(city, sess, queries, today, seen_w, seen_e):
    leads=[]
    if not GOOGLE_OK: return leads
    for q in queries:
        for url in search(q, num_results=20, lang="en"):
            if url in seen_w: continue
            sess.headers.update({"User-Agent": get_ua()})
            try:
                r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
                txt = r.text
                em = EMAIL_RE.search(txt); email = em.group(0) if em else "N/A"
                if email in seen_e: continue
                ph = PHONE_RE.search(txt)
                insta_m = re.search(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9._/-]+", txt)
                insta = insta_m.group(0) if insta_m else "N/A"
                title = BeautifulSoup(txt, "html.parser").title
                leads.append({
                    "Clinic Name": title.text.strip() if title else url,
                    "City": city,
                    "Website": url,
                    "Email": email,
                    "Phone": ph.group(0) if ph else "N/A",
                    "Instagram": insta,
                    "Source URL": url,
                    "Date Added": today
                })
                seen_websites.add(url); seen_emails.add(email)
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                if len(leads)>=target: break
            except:
                pass
        if len(leads)>=target: break
    return leads

# 8) Run scrapers
session = create_session()
today = datetime.date.today().isoformat()
new_leads = []
needed = target

if mode in ("directories","both"):
    for fn in (scrape_whatclinic, scrape_yellowpages):
        for lead in fn(city, session, today):
            if needed<=0: break
            if lead["Website"] in seen_websites or lead["Email"] in seen_emails: continue
            new_leads.append(lead); needed-=1
        if needed<=0: break

if mode in ("google","both") and needed>0:
    queries = [f"dental clinic {city}", f"top dentists {city}", f"orthodontist {city}", f"cosmetic dentist {city}"]
    for lead in scrape_google(city, session, queries, today, seen_websites, seen_emails):
        if needed<=0: break
        new_leads.append(lead); needed-=1

# 9) Append new leads
if new_leads:
    rows = [[l[h] for h in HEADERS] for l in new_leads]
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"💾 Appended {len(new_leads)} new leads to '{ws.title}' tab")
else:
    print("🚫 No new leads to append")
