import streamlit as st
import re, time, random, datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from googlesearch import search
from google.oauth2.service_account import Credentials
import gspread

# --- Config ---
HEADERS = ["Clinic Name", "City", "Website", "Email", "Phone", "Instagram", "Source URL", "Date Added"]
EMAIL_RE = re.compile(r"[\w.\-+]+@[\w.\-+]+\.[A-Za-z]+")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{7,}\d")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)..."
]
MAX_RETRIES, TIMEOUT = 5, 10
DELAY_MIN, DELAY_MAX = 1, 3

# --- Helper functions ---
def get_ua():
    return random.choice(USER_AGENTS)

def create_session():
    sess = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1,
                    status_forcelist=[429,500,502,503,504],
                    respect_retry_after_header=True)
    sess.mount("http://", HTTPAdapter(max_retries=retries))
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.headers.update({"User-Agent": get_ua()})
    return sess

def connect_to_sheet(sheet_url, key_file="service_account.json"):
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("Invalid Google Sheet URL")
    sheet_id = m.group(1)
    creds = Credentials.from_service_account_file(
        key_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def init_worksheet(sh, city):
    names = [ws.title.lower() for ws in sh.worksheets()]
    if city.lower() in names:
        return sh.worksheet(city)
    ws = sh.add_worksheet(title=city, rows="1000", cols="20")
    ws.append_row(HEADERS)
    return ws

def dedupe_sets(ws):
    rows = ws.get_all_values()
    seen_w, seen_e = set(), set()
    if len(rows) > 1:
        hdr = rows[0]
        wi, ei = hdr.index("Website"), hdr.index("Email")
        for r in rows[1:]:
            if len(r) > wi and r[wi]: seen_w.add(r[wi])
            if len(r) > ei and r[ei]: seen_e.add(r[ei])
    return seen_w, seen_e

# --- Scrapers ---
def scrape_whatclinic(city, sess, today):
    url = f"https://www.whatclinic.com/dentists/{city.lower().replace(' ','-')}/pakistan"
    leads = []
    try:
        r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for c in soup.select(".listing"):
            nm = c.select_one("h2")
            lk = c.select_one("a[href]")
            leads.append({
                "Clinic Name": nm.text.strip() if nm else "N/A",
                "City": city,
                "Website": lk["href"] if lk else "N/A",
                "Email": "N/A", "Phone": "N/A", "Instagram": "N/A",
                "Source URL": url, "Date Added": today
            })
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    except:
        pass
    return leads

def scrape_yellowpages(city, sess, today):
    url = f"https://www.yellowpages.com/search?search_terms=dentist&geo_location_terms={city.replace(' ','+')}"
    leads = []
    try:
        r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for c in soup.select(".result"):
            nm = c.select_one("a.business-name")
            ph = c.select_one(".phones")
            leads.append({
                "Clinic Name": nm.text.strip() if nm else "N/A",
                "City": city,
                "Website": nm["href"] if nm and nm.has_attr("href") else "N/A",
                "Email": "N/A", "Phone": ph.text.strip() if ph else "N/A",
                "Instagram": "N/A", "Source URL": url, "Date Added": today
            })
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    except:
        pass
    return leads

def scrape_google(city, sess, queries, today, seen_w, seen_e, target):
    leads = []
    for q in queries:
        for url in search(q, num_results=20, lang="en"):
            if url in seen_w: continue
            try:
                r = sess.get(url, timeout=TIMEOUT); r.raise_for_status()
                txt = r.text
                em = EMAIL_RE.search(txt); email = em.group(0) if em else "N/A"
                if email in seen_e: continue
                ph = PHONE_RE.search(txt)
                insta_m = re.search(r"https?://(?:www\.)?instagram\.com/[\w._-]+", txt)
                leads.append({
                    "Clinic Name": BeautifulSoup(txt, "html.parser").title.text.strip(),
                    "City": city,
                    "Website": url,
                    "Email": email,
                    "Phone": ph.group(0) if ph else "N/A",
                    "Instagram": insta_m.group(0) if insta_m else "N/A",
                    "Source URL": url,
                    "Date Added": today
                })
                seen_w.add(url); seen_e.add(email)
                if len(leads) >= target: return leads
            except:
                pass
    return leads

# --- Master Scrape & Store ---
def scrape_and_store(sheet_url, city, target, mode):
    sh = connect_to_sheet(sheet_url)
    ws = init_worksheet(sh, city)
    seen_w, seen_e = dedupe_sets(ws)
    sess = create_session()
    today = datetime.date.today().isoformat()
    new = []
    needed = target
    if mode in ("directories", "both"):
        for fn in (scrape_whatclinic, scrape_yellowpages):
            for lead in fn(city, sess, today):
                if needed <= 0: break
                if lead["Website"] in seen_w or lead["Email"] in seen_e: continue
                new.append(lead); needed -= 1
            if needed <= 0: break
    if mode in ("google", "both") and needed > 0:
        queries = [f"dental clinic {city}", f"top dentists {city}", f"orthodontist {city}", f"cosmetic dentist {city}"]
        new += scrape_google(city, sess, queries, today, seen_w, seen_e, needed)
    if new:
        rows = [[l[h] for h in HEADERS] for l in new]
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    return new

# --- Dashboard Load & Stats ---
def load_leads(sheet_url, key_file="service_account.json"):
    sh = connect_to_sheet(sheet_url, key_file)
    records = []
    for ws in sh.worksheets():
        vals = ws.get_all_values()
        if len(vals) < 2 or "Date Added" not in vals[0]: continue
        idx = vals[0].index("Date Added")
        for row in vals[1:]:
            try:
                d = pd.to_datetime(row[idx]).date()
                records.append({"Date Added": d, "City": ws.title})
            except: pass
    return pd.DataFrame(records)

def get_dashboard_stats(df):
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)
    total = len(df)
    today_count = df[df["Date Added"] == today].shape[0]
    last_week = df[df["Date Added"] >= week_ago].shape[0]
    by_city = df.groupby("City").size().to_dict()
    return total, today_count, last_week, by_city

# --- Streamlit App ---
st.set_page_config(page_title="Dental Lead Scraper", layout="wide")
st.sidebar.title("Menu")
page = st.sidebar.radio("Select Page", ["Lead Generator", "Dashboard"])

if page == "Lead Generator":
    st.header("Lead Generator")
    sheet_url = st.text_input("Google Sheet URL")
    city = st.text_input("City")
    target = st.number_input("Number of Leads", min_value=1, value=10)
    mode = st.selectbox("Source Mode", ["directories", "google", "both"]
    )
    if st.button("Generate Leads"):
        if not sheet_url or not city:
            st.error("Provide both Sheet URL and City.")
        else:
            with st.spinner("Scraping..."):
                leads = scrape_and_store(sheet_url, city, target, mode)
            if leads:
                st.success(f"Added {len(leads)} leads to '{city}' tab.")
                st.dataframe(pd.DataFrame(leads))
            else:
                st.info("No new leads found.")

elif page == "Dashboard":
    st.header("Dashboard")
    sheet_url = st.text_input("Google Sheet URL", key="dash")
    if sheet_url:
        with st.spinner("Loading..."):
            df = load_leads(sheet_url)
        if df.empty:
            st.warning("No data in sheet.")
        else:
            total, today_count, last_week, by_city = get_dashboard_stats(df)
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Leads", total)
            c2.metric("New Today", today_count)
            c3.metric("Last 7 Days", last_week)
            st.subheader("Leads by City")
            st.bar_chart(pd.Series(by_city))
            st.subheader("Full Log")
            st.dataframe(df.sort_values("Date Added", ascending=False))
            