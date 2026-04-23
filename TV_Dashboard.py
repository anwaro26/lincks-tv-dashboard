import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import time as _time
from datetime import datetime, timedelta
import requests
import base64

st.set_page_config(
    page_title="Lincks TV Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Auto-refresh every 5 minutes ──────────────────────────────────────────────
st.markdown("""
    <meta http-equiv="refresh" content="300">
""", unsafe_allow_html=True)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@300;400;600;700;900&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #0a0a14;
    }
    .stApp { background-color: #0a0a14; }
    [data-testid="stAppViewContainer"] { background-color: #0a0a14; }
    .block-container { padding: 1.5rem 2rem !important; }
    footer { visibility: hidden; }
    #MainMenu { visibility: hidden; }
    header { visibility: hidden; }
    [data-testid="collapsedControl"] { display: none; }

    .tv-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1.5rem;
        padding-bottom: 1rem;
        border-bottom: 2px solid rgba(233,32,118,0.3);
    }
    .tv-title {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 2.8rem;
        color: white;
        letter-spacing: 3px;
        margin: 0;
        line-height: 1;
    }
    .tv-subtitle {
        font-size: 0.85rem;
        color: rgba(255,255,255,0.4);
        margin: 0;
        letter-spacing: 2px;
        text-transform: uppercase;
    }
    .tv-time {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 2rem;
        color: #e92076;
        letter-spacing: 2px;
    }
    .section-label {
        font-size: 0.7rem;
        font-weight: 700;
        color: rgba(255,255,255,0.35);
        text-transform: uppercase;
        letter-spacing: 3px;
        margin-bottom: 0.8rem;
    }
    .stSelectbox > div > div {
        background-color: #1a0a2e !important;
        border: 1px solid rgba(233,32,118,0.3) !important;
        color: white !important;
        font-size: 0.85rem !important;
    }
    .stSelectbox label { color: rgba(255,255,255,0.4) !important; font-size: 0.75rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID     = "4db4f54a9c90230221da81f085ef3bd5.apps.carerix.io"
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
TOKEN_URL     = "https://id-s3.carerix.io/auth/realms/lincks/protocol/openid-connect/token"
API_URL       = "https://api.carerix.io/graphql/v1/graphql"
DATA_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
NL_OFFSET     = timedelta(hours=2)

CONSULTANT_TARGETS = {
    "Mireille Prooi":      0,
    "Renate Leeuwenstein": 35000,
    "Annemieke Bakker":    35000,
    "Arjan Huisman":       35000,
    "Dico Cabout":         31250,
    "Sharon van de Haven": 31250,
    "Sharon Kruijssen":    31250,
    "Rick de Wit":         15000,
    "Rick Wit":            15000,
    "Esther Moerman":      16667,
    "Birgit Lucas":        35000,
    "Nina van Harten":     15000,
    "Nina Harten":         15000,
}
DEFAULT_TARGET = 31250
COMPANY_TARGET = 250000

LINCKS_PERSONEEL_MARGINS = {
    "Bakker Barendrecht":               (0.294693758 + 0.303208773) / 2,
    "Bakker Barendrecht ":              (0.294693758 + 0.303208773) / 2,
    "Nsecure BV":                       (0.424133148 + 0.407688238) / 2,
    "Still Intern Transport":           0.3885738,
    "Beveco Gebouwautomatisering B.V.": 1.0,
}

def nl_now():
    return datetime.utcnow() + NL_OFFSET

def get_token():
    r = requests.post(TOKEN_URL, data={"grant_type": "client_credentials",
                                       "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})
    return r.json()["access_token"]

def run_query(query):
    token = get_token()
    r = requests.post(API_URL, json={"query": query},
                      headers={"Authorization": f"Bearer {token}"}, timeout=60)
    return r.json()

def fetch_new_invoices(since_date):
    probe = run_query("{ crInvoicePage(pageable: {page: 0, size: 1}) { totalElements } }")
    total = (probe.get("data", {}).get("crInvoicePage") or {}).get("totalElements", 0)
    if not total: return []
    page_size = 100
    last_page = (total - 1) // page_size
    pages = list(range(max(0, last_page - 1), last_page + 1))
    all_items = []
    for page_num in pages:
        query = f"""
        {{
            crInvoicePage(pageable: {{page: {page_num}, size: {page_size}}}) {{
                items {{
                    _id invoiceID valueDate date total statusDisplay
                    companyName toCompany {{ name }} toJob {{ _id }} agency {{ name }}
                    toInvoice {{ _id valueDate date }}
                }}
            }}
        }}
        """
        data = run_query(query)
        items = (data.get("data", {}).get("crInvoicePage") or {}).get("items", [])
        for inv in items:
            vd = str(inv.get("valueDate") or "")[:10]
            d  = str(inv.get("date") or "")[:10]
            date_str = max(vd, d) if vd and d else (vd or d)
            if date_str and date_str > since_date:
                all_items.append(inv)
        _time.sleep(0.3)
    return all_items

def get_last_sync():
    meta_path = os.path.join(DATA_DIR, "sync_meta.parquet")
    if os.path.exists(meta_path):
        try:
            meta = pd.read_parquet(meta_path)
            return str(meta["last_incremental_sync"].iloc[0])[:10]
        except: pass
    return "2026-04-02"

@st.cache_data(ttl=3600, show_spinner=False)
def load_parquet_data():
    users_df       = pd.read_parquet(os.path.join(DATA_DIR, "users.parquet"))
    users          = dict(zip(users_df["user_id"], users_df["full_name"]))
    placements_raw = pd.read_parquet(os.path.join(DATA_DIR, "placements.parquet"))
    invoices_raw   = pd.read_parquet(os.path.join(DATA_DIR, "invoices.parquet"))
    return users, placements_raw, invoices_raw

def load_data():
    users, placements_raw, invoices_raw = load_parquet_data()

    if "tv_incremental_done" not in st.session_state:
        st.session_state["tv_incremental_done"] = False

    last_sync  = get_last_sync()
    today      = nl_now().strftime("%Y-%m-%d")

    if last_sync < today and not st.session_state["tv_incremental_done"] and CLIENT_SECRET:
        try:
            new_inv = fetch_new_invoices(last_sync)
            if new_inv:
                rows = []
                for inv in new_inv:
                    company = (inv.get("toCompany") or {}).get("name") or inv.get("companyName") or "Unknown"
                    agency  = (inv.get("agency") or {}).get("name", "Unknown")
                    job     = inv.get("toJob") or {}
                    original = inv.get("toInvoice") or {}
                    rows.append({
                        "invoice_id":    inv.get("invoiceID"),
                        "internal_id":   str(inv["_id"]),
                        "value_date":    inv.get("valueDate"),
                        "date":          inv.get("date"),
                        "original_date": original.get("valueDate") or original.get("date"),
                        "total":         float(inv.get("total") or 0),
                        "status":        inv.get("statusDisplay"),
                        "company":       company,
                        "agency":        agency,
                        "job_id":        str(job.get("_id", "")) if job else "",
                    })
                new_df = pd.DataFrame(rows)
                invoices_raw = pd.concat([invoices_raw, new_df], ignore_index=True).drop_duplicates(subset=["internal_id"], keep="last")
            st.session_state["tv_incremental_done"] = True
            st.session_state["tv_merged_invoices"]  = invoices_raw
        except Exception as e:
            print(f"[TV] Fetch failed: {e}")
    elif st.session_state.get("tv_incremental_done") and "tv_merged_invoices" in st.session_state:
        invoices_raw = st.session_state["tv_merged_invoices"]

    return users, placements_raw, invoices_raw

def build_df(invoices_raw, placements_raw, users):
    job_map = {str(p["placement_id"]): p for _, p in placements_raw.iterrows()}
    rows = []
    for _, inv in invoices_raw.iterrows():
        amount = float(inv.get("total") or 0)
        if amount == 0: continue
        status = str(inv.get("status") or "")
        if status != "Verzonden": continue

        vd   = str(inv.get("value_date") or "")[:10]
        d    = str(inv.get("date") or "")[:10]
        orig = str(inv.get("original_date") or "")[:10]
        if amount < 0 and orig and orig not in ("None", "nan", ""):
            date_str = orig
        else:
            date_str = max(vd, d) if vd and d else (vd or d)
        date_str = date_str.replace("None","").replace("nan","").strip()
        if not date_str: continue
        try: date = pd.to_datetime(date_str)
        except: continue

        company = str(inv.get("company") or "Unknown")
        agency  = str(inv.get("agency") or "Unknown")
        job_id  = str(inv.get("job_id") or "")
        adjusted = amount * LINCKS_PERSONEEL_MARGINS.get(company, 1.0) if agency == "Lincks Personeel B.V." else amount

        consultants = []
        if job_id and job_id in job_map:
            p = job_map[job_id]
            fase_users = list(set([str(p[k]) for k in ["fase_6525","fase_6526","fase_6527","fase_6528","fase_6538"]
                                   if k in p.index and str(p[k]) not in ("","nan","None")]))
            consultants = [users.get(uid, f"User {uid}") for uid in fase_users]
        if not consultants: consultants = ["Mireille Prooi"]

        rev_per = adjusted / len(consultants)
        for c in consultants:
            rows.append({"consultant": c, "amount": adjusted, "revenue": rev_per,
                         "date": date, "month": date.strftime("%Y-%m"), "year": date.year})
    return pd.DataFrame(rows)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner(""):
    try:
        users, placements_raw, invoices_raw = load_data()
        df = build_df(invoices_raw, placements_raw, users)
        data_loaded = True
    except Exception as e:
        st.error(f"Error: {e}")
        data_loaded = False

if not data_loaded or df.empty:
    st.warning("Geen data beschikbaar.")
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
now = nl_now()
logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lincks_logo_fc-wit_def.png")
logo_html = ""
if os.path.exists(logo_path):
    with open(logo_path, "rb") as f:
        logo_b64 = base64.b64encode(f.read()).decode()
    logo_html = f'<img src="data:image/png;base64,{logo_b64}" style="height:52px;" />'

st.markdown(f"""
<div class="tv-header">
    <div>
        {logo_html}
        <p class="tv-subtitle" style="margin-top:4px;">Performance Dashboard</p>
    </div>
    <div style="text-align:right;">
        <div class="tv-time">{now.strftime("%H:%M")}</div>
        <div class="tv-subtitle">{now.strftime("%A %d %B %Y")}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Month selector ────────────────────────────────────────────────────────────
all_months = sorted(df["month"].unique().tolist(), reverse=True)
current_month = now.strftime("%Y-%m")
default_month = current_month if current_month in all_months else all_months[0]

col_sel, _ = st.columns([1, 4])
with col_sel:
    selected_month = st.selectbox("📆 Maand", all_months,
                                   index=all_months.index(default_month))

filtered = df[df["month"] == selected_month]
inv_filtered = filtered.drop_duplicates(subset=["consultant", "amount", "date"])

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_rev = filtered.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
pct_company = min(total_rev / COMPANY_TARGET * 100, 100)
remaining = max(COMPANY_TARGET - total_rev, 0)

# ── Layout: Donut left, Bar right ─────────────────────────────────────────────
col_donut, col_bar = st.columns([1, 2])

with col_donut:
    st.markdown('<div class="section-label">Maandtarget Bedrijf</div>', unsafe_allow_html=True)

    fig_donut = go.Figure(go.Pie(
        values=[total_rev, remaining],
        labels=["Behaald", "Resterend"],
        hole=0.75,
        marker_colors=["#e92076", "rgba(255,255,255,0.06)"],
        textinfo="none",
        hovertemplate="%{label}: €%{value:,.0f}<extra></extra>",
        sort=False,
    ))
    fig_donut.add_annotation(
        text=f"<b>{pct_company:.0f}%</b>",
        x=0.5, y=0.60,
        font=dict(size=64, color="#e92076", family="Bebas Neue"),
        showarrow=False
    )
    fig_donut.add_annotation(
        text=f"€{total_rev:,.0f}",
        x=0.5, y=0.42,
        font=dict(size=22, color="rgba(255,255,255,0.8)"),
        showarrow=False
    )
    fig_donut.add_annotation(
        text=f"van €{COMPANY_TARGET:,.0f}",
        x=0.5, y=0.28,
        font=dict(size=14, color="rgba(255,255,255,0.4)"),
        showarrow=False
    )
    fig_donut.update_layout(
        plot_bgcolor="#0a0a14", paper_bgcolor="#0a0a14",
        showlegend=False,
        margin=dict(l=20, r=20, t=20, b=20),
        height=420,
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col_bar:
    st.markdown('<div class="section-label">Omzet per Recruiter</div>', unsafe_allow_html=True)

    rev_con = (filtered[~filtered["consultant"].isin(["Mireille Prooi"])]
               .groupby("consultant")["revenue"].sum().reset_index())

    def get_target(name):
        if name in CONSULTANT_TARGETS: return CONSULTANT_TARGETS[name]
        last = name.split()[-1] if name else ""
        for k, v in CONSULTANT_TARGETS.items():
            if k.split()[-1] == last: return v
        return DEFAULT_TARGET

    rev_con["target"]    = rev_con["consultant"].apply(get_target)
    rev_con["pct"]       = (rev_con["revenue"] / rev_con["target"] * 100).round(1)
    rev_con["resterend"] = (rev_con["target"] - rev_con["revenue"]).clip(lower=0)
    rev_con              = rev_con.sort_values("revenue", ascending=True)
    rev_con["color"]     = rev_con["pct"].apply(
        lambda x: "#00e5a0" if x >= 100 else ("#f5a623" if x >= 80 else "#e92076"))

    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        x=rev_con["revenue"], y=rev_con["consultant"],
        orientation="h", name="Behaald",
        marker_color=rev_con["color"], marker_line_width=0,
        text=rev_con.apply(lambda r: f"€{r['revenue']:,.0f}  ({r['pct']:.0f}%)", axis=1),
        textposition="inside", insidetextanchor="middle",
        textfont=dict(color="white", size=15, family="Inter"),
        hovertemplate="<b>%{y}</b><br>€%{x:,.0f}<extra></extra>",
    ))
    fig_bar.add_trace(go.Bar(
        x=rev_con["resterend"], y=rev_con["consultant"],
        orientation="h", name="Resterend",
        marker_color="rgba(255,255,255,0.05)",
        marker_line_color="rgba(255,255,255,0.1)", marker_line_width=1,
        hovertemplate="<b>%{y}</b><br>Nog: €%{x:,.0f}<extra></extra>",
    ))
    fig_bar.update_layout(
        barmode="stack",
        plot_bgcolor="#0a0a14", paper_bgcolor="#0a0a14",
        font_color="rgba(255,255,255,0.6)",
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickprefix="€",
                   tickfont=dict(size=12)),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickfont=dict(size=15)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(color="white", size=12)),
        margin=dict(l=0, r=20, t=40, b=0),
        height=420,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center; margin-top:1rem; color:rgba(255,255,255,0.15);
     font-size:0.7rem; letter-spacing:2px; text-transform:uppercase;">
    Automatisch vernieuwd om {now.strftime("%H:%M")} · Elke 5 minuten
</div>
""", unsafe_allow_html=True)