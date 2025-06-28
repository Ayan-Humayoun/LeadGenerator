# Dashboard Cell: Live Google Sheets → Lead Dashboard (robust to duplicate headers)

# 1) Install & import (one-time per session)


from google.colab import auth
auth.authenticate_user()

import gspread, google.auth, pandas as pd, re
from datetime import date, timedelta
from IPython.display import display, Markdown

# 2) Open your sheet by pasting the URL
sheet_url = input("📑 Paste Google Sheet URL: ").strip()
m = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
if not m:
    raise ValueError("Invalid Google Sheet URL")
sheet_id = m.group(1)

creds, _ = google.auth.default()
gc = gspread.authorize(creds)
sh = gc.open_by_key(sheet_id)

# 3) Load & combine all city tabs (manual parsing)
records = []
for ws in sh.worksheets():
    vals = ws.get_all_values()
    if len(vals) < 2:
        continue
    header = vals[0]
    if "Date Added" not in header:
        continue
    date_idx = header.index("Date Added")
    # parse each data row
    for row in vals[1:]:
        cell = row[date_idx] if date_idx < len(row) else ""
        if not cell:
            continue
        try:
            d = pd.to_datetime(cell).date()
        except:
            continue
        records.append({"Date Added": d, "City": ws.title})

if not records:
    display(Markdown("🚫 **No valid lead data found in any tab.**"))
else:
    df_all = pd.DataFrame(records)

    # 4) Lead Dashboard Summary
    today = date.today()
    week_ago = today - timedelta(days=7)
    total_leads  = len(df_all)
    new_today    = df_all[df_all["Date Added"] == today].shape[0]
    last_7_days  = df_all[df_all["Date Added"] >= week_ago].shape[0]

    display(Markdown(f"""
**📊 Lead Dashboard Summary**
- **Total Leads:** {total_leads}
- **New Today ({today}):** {new_today}
- **Leads in Last 7 Days:** {last_7_days}
"""))

    # 5) Ads Comparison (Yesterday → Today)
    yesterday = today - timedelta(days=1)
    grouped = df_all.groupby(["Date Added","City"]).size().reset_index(name="Leads")
    pivot  = grouped.pivot(index="City", columns="Date Added", values="Leads").fillna(0)
    for d in (yesterday, today):
        if d not in pivot.columns:
            pivot[d] = 0
    pivot["Change"] = pivot[today] - pivot[yesterday]
    comp = pivot.reset_index()[["City", yesterday, today, "Change"]]
    comp.columns = ["City","Yesterday","Today","Change"]

    display(Markdown(f"**📅 Ads Comparison ({yesterday} → {today}):**"))
    display(comp)

    # 6) Full Lead Log (Grouped by Date & City)
    display(Markdown("**📋 Full Lead Log (Grouped by Date & City):**"))
    display(grouped.sort_values(["Date Added","City"]).reset_index(drop=True))
