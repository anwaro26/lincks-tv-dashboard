import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
import time as _time
from datetime import datetime, timedelta
import requests
import base64
import json

st.set_page_config(
    page_title="Lincks Performance",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;700&family=Space+Grotesk:wght@400;500;700&family=Syne:wght@700;800&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background: #050510;
    color: white;
}
.stApp, [data-testid="stAppViewContainer"] { background: #050510; }
.block-container { padding: 0 !important; max-width: 100% !important; }
footer, #MainMenu, header, [data-testid="collapsedControl"] { visibility: hidden !important; display: none !important; }
section[data-testid="stSidebar"] { display: none !important; }

/* Background grid */
.stApp::before {
    content: '';
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background-image:
        linear-gradient(rgba(233,32,118,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(233,32,118,0.03) 1px, transparent 1px);
    background-size: 60px 60px;
    pointer-events: none; z-index: 0;
}

.dashboard-wrap { position: relative; z-index: 1; padding: 2rem 2.5rem; min-height: 100vh; }

/* Header */
.hdr {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 2rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}
.hdr-left { display: flex; align-items: center; gap: 1.5rem; }
.hdr-badge {
    background: linear-gradient(135deg, #e92076, #c41660);
    color: white; font-family: 'Syne', sans-serif;
    font-size: 0.6rem; font-weight: 800; letter-spacing: 3px;
    padding: 0.3rem 0.8rem; border-radius: 4px; text-transform: uppercase;
}
.hdr-right { text-align: right; }
.clock {
    font-family: 'Syne', sans-serif; font-size: 3rem; font-weight: 800;
    color: #e92076; line-height: 1; letter-spacing: -1px;
}
.clock-date { font-size: 0.72rem; color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase; margin-top: 2px; }

/* Screen nav */
.screen-nav {
    display: flex; gap: 0.5rem; margin-bottom: 2rem; align-items: center;
}
.screen-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: rgba(255,255,255,0.15); transition: all 0.3s;
}
.screen-dot.active { background: #e92076; box-shadow: 0 0 12px rgba(233,32,118,0.6); width: 24px; border-radius: 4px; }
.screen-label {
    font-size: 0.65rem; color: rgba(255,255,255,0.3); letter-spacing: 3px;
    text-transform: uppercase; margin-left: 1rem;
}

/* Cards */
.card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 16px; padding: 1.5rem;
    position: relative; overflow: hidden;
    backdrop-filter: blur(10px);
}
.card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(233,32,118,0.5), transparent);
}
.card-label {
    font-size: 0.65rem; font-weight: 700; color: rgba(255,255,255,0.3);
    text-transform: uppercase; letter-spacing: 3px; margin-bottom: 0.8rem;
}
.card-val { font-family: 'Syne', sans-serif; font-size: 3.2rem; font-weight: 800; line-height: 1; }
.card-val.pink { color: #e92076; }
.card-val.teal { color: #00d4c8; }
.card-val.white { color: white; }
.card-sub { font-size: 0.75rem; color: rgba(255,255,255,0.3); margin-top: 0.4rem; }

/* Progress bar */
.prog-wrap { margin-top: 1rem; }
.prog-track { height: 4px; background: rgba(255,255,255,0.06); border-radius: 4px; overflow: hidden; }
.prog-fill { height: 100%; border-radius: 4px; background: linear-gradient(90deg, #e92076, #ff6ab0); transition: width 1s ease; }

/* Vacancy card */
.vac-card {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px; padding: 1rem 1.2rem;
    margin-bottom: 0.7rem;
    border-left: 3px solid #e92076;
    transition: border-color 0.2s;
}
.vac-title { font-size: 0.95rem; font-weight: 600; color: white; margin-bottom: 0.3rem; }
.vac-meta { font-size: 0.72rem; color: rgba(255,255,255,0.35); display: flex; gap: 1rem; flex-wrap: wrap; }
.vac-meta span { display: flex; align-items: center; gap: 0.3rem; }

/* Funnel KPI row */
.funnel-kpis { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1rem; margin-bottom: 2rem; }
.fkpi {
    background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px; padding: 1.5rem 1rem; text-align: center;
    position: relative; overflow: hidden;
}
.fkpi::after { content: ''; position: absolute; bottom: 0; left: 20%; right: 20%; height: 2px; border-radius: 2px; }
.fkpi.c1::after { background: #00d4c8; }
.fkpi.c2::after { background: #e92076; }
.fkpi.c3::after { background: #f5a623; }
.fkpi.c4::after { background: #a78bfa; }
.fkpi.c5::after { background: #00e5a0; }
.fkpi-val { font-family: 'Syne', sans-serif; font-size: 3.5rem; font-weight: 800; line-height: 1; }
.fkpi-label { font-size: 0.65rem; color: rgba(255,255,255,0.35); letter-spacing: 2px; text-transform: uppercase; margin-top: 0.5rem; }

/* Plotly override */
.js-plotly-plot .plotly { background: transparent !important; }

/* StSelectbox */
.stSelectbox > div > div { background: rgba(255,255,255,0.04) !important; border: 1px solid rgba(255,255,255,0.1) !important; color: white !important; border-radius: 8px !important; font-size: 0.8rem !important; }
.stSelectbox label { color: rgba(255,255,255,0.3) !important; font-size: 0.65rem !important; letter-spacing: 2px !important; text-transform: uppercase !important; }
.stButton > button { background: rgba(255,255,255,0.05) !important; color: rgba(255,255,255,0.5) !important; border: 1px solid rgba(255,255,255,0.1) !important; border-radius: 8px !important; font-size: 0.72rem !important; letter-spacing: 1px !important; text-transform: uppercase !important; }
.stButton > button:hover { background: rgba(233,32,118,0.2) !important; border-color: rgba(233,32,118,0.4) !important; color: white !important; }
</style>
""", unsafe_allow_html=True)

# ── JS: live clock + auto-switch ──────────────────────────────────────────────
st.markdown("""
<script>
const NL_D=['Zondag','Maandag','Dinsdag','Woensdag','Donderdag','Vrijdag','Zaterdag'];
const NL_M=['januari','februari','maart','april','mei','juni','juli','augustus','september','oktober','november','december'];
function getNL(){ return new Date(new Date().toLocaleString("en-US",{timeZone:"Europe/Amsterdam"})); }
function tick(){
    const n=getNL();
    const c=String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0')+':'+String(n.getSeconds()).padStart(2,'0');
    const d=NL_D[n.getDay()]+' '+n.getDate()+' '+NL_M[n.getMonth()]+' '+n.getFullYear();
    const el=document.getElementById('js-clock'); if(el) el.textContent=c;
    const el2=document.getElementById('js-date'); if(el2) el2.textContent=d.toUpperCase();
}
tick(); setInterval(tick,1000);
// Auto reload every 30 seconds for screen switching
setTimeout(function(){ window.location.reload(); }, 30000);
</script>
""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID     = "4db4f54a9c90230221da81f085ef3bd5.apps.carerix.io"
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
TOKEN_URL     = "https://id-s3.carerix.io/auth/realms/lincks/protocol/openid-connect/token"
API_URL       = "https://api.carerix.io/graphql/v1/graphql"
DATA_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

CONSULTANT_TARGETS = {
    "Mireille Prooi": 0,
    "Renate Leeuwenstein": 35000, "Annemieke Bakker": 35000, "Arjan Huisman": 35000,
    "Dico Cabout": 31250, "Sharon van de Haven": 31250, "Sharon Kruijssen": 31250,
    "Rick de Wit": 15000, "Rick Wit": 15000, "Esther Moerman": 16667,
    "Birgit Lucas": 35000, "Nina van Harten": 15000, "Nina Harten": 15000,
}
DEFAULT_TARGET = 31250
COMPANY_TARGET = 250000
LINCKS_PERSONEEL_MARGINS = {
    "Bakker Barendrecht": (0.294693758+0.303208773)/2,
    "Bakker Barendrecht ": (0.294693758+0.303208773)/2,
    "Nsecure BV": (0.424133148+0.407688238)/2,
    "Still Intern Transport": 0.3885738,
    "Beveco Gebouwautomatisering B.V.": 1.0,
}

def nl_now(): return datetime.utcnow() + timedelta(hours=2)

def get_token():
    r = requests.post(TOKEN_URL, data={"grant_type":"client_credentials","client_id":CLIENT_ID,"client_secret":CLIENT_SECRET})
    return r.json()["access_token"]

def run_query(q):
    t = get_token()
    r = requests.post(API_URL, json={"query":q}, headers={"Authorization":f"Bearer {t}"}, timeout=60)
    return r.json()

def get_last_sync():
    p = os.path.join(DATA_DIR,"sync_meta.parquet")
    if os.path.exists(p):
        try: return str(pd.read_parquet(p)["last_incremental_sync"].iloc[0])[:10]
        except: pass
    return "2026-04-02"

@st.cache_data(ttl=3600, show_spinner=False)
def load_parquet():
    u = pd.read_parquet(os.path.join(DATA_DIR,"users.parquet"))
    users = dict(zip(u["user_id"], u["full_name"]))
    pl = pd.read_parquet(os.path.join(DATA_DIR,"placements.parquet"))
    inv = pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet"))
    return users, pl, inv

def fetch_new_invoices(since):
    probe = run_query("{ crInvoicePage(pageable:{page:0,size:1}){ totalElements } }")
    total = (probe.get("data",{}).get("crInvoicePage") or {}).get("totalElements",0)
    if not total: return []
    lp = (total-1)//100
    all_items = []
    for pn in range(max(0,lp-1), lp+1):
        q = f"""{{ crInvoicePage(pageable:{{page:{pn},size:100}}){{
            items{{ _id invoiceID valueDate date total statusDisplay companyName
                   toCompany{{name}} toJob{{_id}} agency{{name}} toInvoice{{_id valueDate date}} }}
        }} }}"""
        data = run_query(q)
        items = (data.get("data",{}).get("crInvoicePage") or {}).get("items",[])
        for inv in items:
            vd = str(inv.get("valueDate") or "")[:10]
            d  = str(inv.get("date") or "")[:10]
            ds = max(vd,d) if vd and d else (vd or d)
            if ds and ds > since: all_items.append(inv)
        _time.sleep(0.3)
    return all_items

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_open_vacs():
    closed = ["Geplaatst","Verloren","Vervallen"]
    result = []
    for pn in range(5):
        q = f"""{{ crVacancyPage(pageable:{{page:{pn},size:100}}){{
            totalElements items{{ _id creationDate statusDisplay jobTitle vacancyNo
                toStatusNode{{label}} toCompany{{name}} owner{{firstName lastName}} matchCount agency{{name}} }}
        }} }}"""
        data = run_query(q)
        if not data or not data.get("data"): break
        page = data.get("data",{}).get("crVacancyPage") or {}
        items = page.get("items",[])
        if not items: break
        for v in items:
            if v.get("statusDisplay") not in closed: result.append(v)
        if len(result) >= page.get("totalElements",0): break
        _time.sleep(0.2)
    return result

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_funnel(month_start):
    stats = {"nieuwe_vacatures":0,"eerste_gesprek":0,"tweede_gesprek":0,"aanbod":0,"geplaatst":0}
    try:
        q1 = f"""{{ crMatchPage(qualifier:"creationDate > (NSCalendarDate) '{month_start} 00:00:00'",pageable:{{page:0,size:500}}){{
            totalElements items{{ _id statusInfo{{ name displayName }} }} }} }}"""
        d1 = run_query(q1)
        for m in (d1.get("data",{}).get("crMatchPage") or {}).get("items",[]):
            sn = str((m.get("statusInfo") or {}).get("displayName") or "")
            if "4.1" in sn: stats["eerste_gesprek"] += 1
            elif "4.2" in sn: stats["tweede_gesprek"] += 1
            elif "5.0" in sn: stats["aanbod"] += 1

        q2 = f"""{{ crJobPage(qualifier:"creationDate > (NSCalendarDate) '{month_start} 00:00:00'",pageable:{{page:0,size:1}}){{ totalElements }} }}"""
        stats["geplaatst"] = (run_query(q2).get("data",{}).get("crJobPage") or {}).get("totalElements",0)

        q3 = f"""{{ crVacancyPage(qualifier:"creationDate > (NSCalendarDate) '{month_start} 00:00:00'",pageable:{{page:0,size:1}}){{ totalElements }} }}"""
        stats["nieuwe_vacatures"] = (run_query(q3).get("data",{}).get("crVacancyPage") or {}).get("totalElements",0)
    except Exception as e:
        print(f"[FUNNEL] {e}")
    return stats

def load_data():
    users, pl, inv = load_parquet()
    if "tv_done" not in st.session_state: st.session_state["tv_done"] = False
    last_sync = get_last_sync()
    today = nl_now().strftime("%Y-%m-%d")
    if last_sync < today and not st.session_state["tv_done"] and CLIENT_SECRET:
        try:
            new_inv = fetch_new_invoices(last_sync)
            if new_inv:
                rows = []
                for i in new_inv:
                    co = (i.get("toCompany") or {}).get("name") or i.get("companyName") or "Unknown"
                    ag = (i.get("agency") or {}).get("name","Unknown")
                    jb = i.get("toJob") or {}
                    og = i.get("toInvoice") or {}
                    rows.append({"invoice_id":i.get("invoiceID"),"internal_id":str(i["_id"]),
                                 "value_date":i.get("valueDate"),"date":i.get("date"),
                                 "original_date":og.get("valueDate") or og.get("date"),
                                 "total":float(i.get("total") or 0),"status":i.get("statusDisplay"),
                                 "company":co,"agency":ag,"job_id":str(jb.get("_id","")) if jb else ""})
                new_df = pd.DataFrame(rows)
                inv = pd.concat([inv,new_df],ignore_index=True).drop_duplicates(subset=["internal_id"],keep="last")
            st.session_state["tv_done"] = True
            st.session_state["tv_inv"] = inv
        except Exception as e:
            print(f"[TV] {e}")
    elif st.session_state.get("tv_done") and "tv_inv" in st.session_state:
        inv = st.session_state["tv_inv"]
    return users, pl, inv

def build_df(inv, pl, users):
    job_map = {str(p["placement_id"]): p for _, p in pl.iterrows()}
    rows = []
    for _, i in inv.iterrows():
        amt = float(i.get("total") or 0)
        if amt == 0: continue
        if str(i.get("status") or "") != "Verzonden": continue
        vd = str(i.get("value_date") or "")[:10]
        d  = str(i.get("date") or "")[:10]
        og = str(i.get("original_date") or "")[:10]
        if amt < 0 and og and og not in ("None","nan",""):
            ds = og
        else:
            ds = max(vd,d) if vd and d else (vd or d)
        ds = ds.replace("None","").replace("nan","").strip()
        if not ds: continue
        try: date = pd.to_datetime(ds)
        except: continue
        co = str(i.get("company") or "Unknown")
        ag = str(i.get("agency") or "Unknown")
        jid = str(i.get("job_id") or "")
        adj = amt * LINCKS_PERSONEEL_MARGINS.get(co,1.0) if ag == "Lincks Personeel B.V." else amt
        cons = []
        if jid and jid in job_map:
            p = job_map[jid]
            fu = list(set([str(p[k]) for k in ["fase_6525","fase_6526","fase_6527","fase_6528","fase_6538"]
                           if k in p.index and str(p[k]) not in ("","nan","None")]))
            cons = [users.get(u, f"User {u}") for u in fu]
        if not cons: cons = ["Mireille Prooi"]
        rp = adj / len(cons)
        for c in cons:
            rows.append({"consultant":c,"amount":adj,"revenue":rp,"date":date,"month":date.strftime("%Y-%m"),"year":date.year})
    return pd.DataFrame(rows)

# ── Load ──────────────────────────────────────────────────────────────────────
with st.spinner(""):
    try:
        users, pl, inv = load_data()
        df = build_df(inv, pl, users)
        data_ok = True
    except Exception as e:
        st.error(f"Error: {e}")
        data_ok = False

if not data_ok or df.empty:
    st.warning("Geen data.")
    st.stop()

# ── Screen state ──────────────────────────────────────────────────────────────
if "screen" not in st.session_state: st.session_state["screen"] = 0
if "last_sw" not in st.session_state: st.session_state["last_sw"] = _time.time()
if _time.time() - st.session_state["last_sw"] > 30:
    st.session_state["screen"] = (st.session_state["screen"] + 1) % 3
    st.session_state["last_sw"] = _time.time()

# ── Month ─────────────────────────────────────────────────────────────────────
all_months = sorted(df["month"].unique().tolist(), reverse=True)
now_m = nl_now().strftime("%Y-%m")
def_m = now_m if now_m in all_months else all_months[0]

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lincks_logo_fc-wit_def.png")
logo_html = ""
if os.path.exists(logo_path):
    with open(logo_path,"rb") as f: lb64 = base64.b64encode(f.read()).decode()
    logo_html = f'<img src="data:image/png;base64,{lb64}" style="height:44px;" />'
else:
    logo_html = '<span style="font-family:Syne,sans-serif;font-size:1.6rem;font-weight:800;color:white;letter-spacing:-1px;">LINCKS</span>'

# ════════════════════════════════════════════════════════════════
# LAYOUT
# ════════════════════════════════════════════════════════════════
st.markdown('<div class="dashboard-wrap">', unsafe_allow_html=True)

# Header
scr = st.session_state["screen"]
screen_names = ["Omzet","Funnel","Vacatures"]
st.markdown(f"""
<div class="hdr">
    <div class="hdr-left">
        {logo_html}
        <div>
            <div class="hdr-badge">Performance</div>
        </div>
    </div>
    <div class="hdr-right">
        <div class="clock" id="js-clock">--:--:--</div>
        <div class="clock-date" id="js-date">--</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Nav dots + controls
dots_html = "".join([f'<div class="screen-dot {"active" if i==scr else ""}"></div>' for i in range(3)])
st.markdown(f'<div class="screen-nav">{dots_html}<span class="screen-label">{screen_names[scr]}</span></div>', unsafe_allow_html=True)

# Control row
c1,c2,c3,c4,_sp,mc,ec = st.columns([0.8,0.8,0.8,0.8,2,1.5,1.5])
with c1:
    if st.button("💰 Omzet"):
        st.session_state["screen"]=0; st.session_state["last_sw"]=_time.time(); st.rerun()
with c2:
    if st.button("📊 Funnel"):
        st.session_state["screen"]=1; st.session_state["last_sw"]=_time.time(); st.rerun()
with c3:
    if st.button("📋 Vacatures"):
        st.session_state["screen"]=2; st.session_state["last_sw"]=_time.time(); st.rerun()
with c4:
    if st.button("🔄"):
        st.cache_data.clear()
        for k in ["tv_done","tv_inv"]: st.session_state.pop(k,None)
        st.rerun()
with mc:
    sel_m = st.selectbox("Maand", all_months, index=all_months.index(def_m), label_visibility="collapsed")
with ec:
    st.markdown("""<a href="https://www.lincks.nl/vacatures" target="_blank"
       style="display:block;text-align:center;background:rgba(255,255,255,0.04);
       border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:0.5rem;
       color:rgba(255,255,255,0.4);text-decoration:none;font-size:0.7rem;
       letter-spacing:1px;text-transform:uppercase;margin-top:2px;">🌐 Vacaturesite ↗</a>""",
       unsafe_allow_html=True)

st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 0 — OMZET
# ════════════════════════════════════════════════════════════════
if scr == 0:
    filtered  = df[df["month"] == sel_m]
    total_rev = filtered.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
    pct       = min(total_rev / COMPANY_TARGET * 100, 100)
    remaining = max(COMPANY_TARGET - total_rev, 0)

    # Top KPIs
    k1,k2,k3 = st.columns(3)
    with k1:
        num_inv = len(filtered.drop_duplicates(subset=["consultant","amount","date"]))
        st.markdown(f"""<div class="card">
            <div class="card-label">Maandomzet</div>
            <div class="card-val pink">€{total_rev:,.0f}</div>
            <div class="card-sub">{num_inv} facturen · {sel_m}</div>
            <div class="prog-wrap"><div class="prog-track"><div class="prog-fill" style="width:{pct}%"></div></div></div>
        </div>""", unsafe_allow_html=True)
    with k2:
        excl = filtered[~filtered["consultant"].isin(["Mireille Prooi","Onbekend"])]
        top_c = excl.groupby("consultant")["revenue"].sum().idxmax() if not excl.empty else "—"
        top_v = excl.groupby("consultant")["revenue"].sum().max() if not excl.empty else 0
        st.markdown(f"""<div class="card">
            <div class="card-label">Top Recruiter</div>
            <div class="card-val white" style="font-size:2rem;margin-top:0.3rem;">{top_c}</div>
            <div class="card-sub">€{top_v:,.0f} deze maand</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div class="card">
            <div class="card-label">Target Voortgang</div>
            <div class="card-val {"teal" if pct>=100 else "pink"}">{pct:.0f}%</div>
            <div class="card-sub">€{remaining:,.0f} te gaan · doel €{COMPANY_TARGET:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

    # Charts
    col_donut, col_bar = st.columns([1, 2.2])

    with col_donut:
        fig = go.Figure(go.Pie(
            values=[total_rev, remaining], labels=["Behaald","Resterend"],
            hole=0.78, sort=False, textinfo="none",
            marker_colors=["#e92076","rgba(255,255,255,0.04)"],
            marker=dict(line=dict(width=0)),
        ))
        fig.add_annotation(text=f"<b>{pct:.0f}%</b>", x=0.5, y=0.58,
            font=dict(size=60, color="#e92076", family="Syne"), showarrow=False)
        fig.add_annotation(text=f"€{total_rev:,.0f}", x=0.5, y=0.40,
            font=dict(size=18, color="rgba(255,255,255,0.7)"), showarrow=False)
        fig.add_annotation(text=f"van €{COMPANY_TARGET:,.0f}", x=0.5, y=0.26,
            font=dict(size=12, color="rgba(255,255,255,0.3)"), showarrow=False)
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False, margin=dict(l=10,r=10,t=10,b=10), height=360)
        st.plotly_chart(fig, use_container_width=True)

    with col_bar:
        def get_tgt(name):
            if name in CONSULTANT_TARGETS: return CONSULTANT_TARGETS[name]
            last = name.split()[-1] if name else ""
            for k,v in CONSULTANT_TARGETS.items():
                if k.split()[-1] == last: return v
            return DEFAULT_TARGET

        rc = (filtered[~filtered["consultant"].isin(["Mireille Prooi"])]
              .groupby("consultant")["revenue"].sum().reset_index())
        rc["target"]  = rc["consultant"].apply(get_tgt)
        rc["pct"]     = (rc["revenue"]/rc["target"]*100).round(1)
        rc["rest"]    = (rc["target"]-rc["revenue"]).clip(lower=0)
        rc = rc.sort_values("revenue", ascending=True)
        rc["color"]   = rc["pct"].apply(lambda x: "#00e5a0" if x>=100 else ("#f5a623" if x>=80 else "#e92076"))

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=rc["revenue"], y=rc["consultant"], orientation="h", name="Behaald",
            marker_color=rc["color"], marker_line_width=0,
            text=rc.apply(lambda r: f"  €{r['revenue']:,.0f}  ·  {r['pct']:.0f}%", axis=1),
            textposition="inside", insidetextanchor="start",
            textfont=dict(color="white", size=13, family="DM Sans"),
            hovertemplate="<b>%{y}</b><br>€%{x:,.0f}<extra></extra>",
        ))
        fig2.add_trace(go.Bar(
            x=rc["rest"], y=rc["consultant"], orientation="h", name="Resterend",
            marker_color="rgba(255,255,255,0.03)",
            marker_line_color="rgba(255,255,255,0.08)", marker_line_width=1,
            hovertemplate="<b>%{y}</b><br>Nog: €%{x:,.0f}<extra></extra>",
        ))
        fig2.update_layout(
            barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.5)",
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickprefix="€", tickfont=dict(size=11), zeroline=False),
            yaxis=dict(tickfont=dict(size=14, family="DM Sans"), gridcolor="rgba(0,0,0,0)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
                        font=dict(color="rgba(255,255,255,0.4)", size=11)),
            margin=dict(l=0,r=10,t=30,b=0), height=360, bargap=0.25,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 1 — FUNNEL
# ════════════════════════════════════════════════════════════════
elif scr == 1:
    month_start = f"{sel_m}-01"
    with st.spinner(""):
        stats = fetch_funnel(month_start)

    # KPI row
    kpi_items = [
        ("Nieuwe Vacatures", stats["nieuwe_vacatures"], "#00d4c8", "c1"),
        ("1e Gesprek Klant",  stats["eerste_gesprek"],  "#e92076", "c2"),
        ("2e Gesprek Klant",  stats["tweede_gesprek"],  "#f5a623", "c3"),
        ("Heeft Aanbod",      stats["aanbod"],          "#a78bfa", "c4"),
        ("Geplaatst ✓",       stats["geplaatst"],       "#00e5a0", "c5"),
    ]
    cols = st.columns(5)
    for col, (lbl, val, clr, cls) in zip(cols, kpi_items):
        with col:
            st.markdown(f"""<div class="fkpi {cls}">
                <div class="fkpi-val" style="color:{clr}">{val}</div>
                <div class="fkpi-label">{lbl}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:2rem'></div>", unsafe_allow_html=True)

    # Funnel chart
    labels  = ["Nieuwe Vacatures","1e Gesprek","2e Gesprek","Aanbod","Geplaatst"]
    values  = [stats["nieuwe_vacatures"], stats["eerste_gesprek"],
               stats["tweede_gesprek"], stats["aanbod"], stats["geplaatst"]]
    colors  = ["#00d4c8","#e92076","#f5a623","#a78bfa","#00e5a0"]

    fig3 = go.Figure(go.Funnel(
        y=labels, x=values, textinfo="value+percent initial",
        marker=dict(color=colors, line=dict(width=0)),
        connector=dict(line=dict(color="rgba(255,255,255,0.06)", width=2)),
        textfont=dict(size=17, color="white", family="Syne"),
    ))
    fig3.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="rgba(255,255,255,0.6)",
        margin=dict(l=200,r=80,t=20,b=20), height=360,
        yaxis=dict(tickfont=dict(size=15, family="DM Sans")),
    )
    st.plotly_chart(fig3, use_container_width=True)

    # Month comparison (current vs previous)
    filtered_now  = df[df["month"] == sel_m]
    prev_m_idx = all_months.index(sel_m)
    prev_total = 0
    if prev_m_idx + 1 < len(all_months):
        prev_m = all_months[prev_m_idx + 1]
        prev_total = df[df["month"]==prev_m].drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
    now_total = filtered_now.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
    delta = now_total - prev_total
    delta_pct = (delta/prev_total*100) if prev_total > 0 else 0
    arrow = "↑" if delta >= 0 else "↓"
    col = "#00e5a0" if delta >= 0 else "#e92076"
    st.markdown(f"""
    <div class="card" style="text-align:center;padding:1rem 2rem;">
        <div class="card-label">Omzet vs vorige maand</div>
        <div style="font-family:Syne,sans-serif;font-size:2rem;font-weight:800;color:{col}">
            {arrow} €{abs(delta):,.0f} &nbsp;<span style="font-size:1rem;opacity:0.7">({delta_pct:+.1f}%)</span>
        </div>
    </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 2 — VACATURES
# ════════════════════════════════════════════════════════════════
elif scr == 2:
    with st.spinner(""):
        open_vacs = fetch_open_vacs()

    now = nl_now()
    vrows = []
    for v in open_vacs:
        cr = pd.to_datetime(v.get("creationDate")) if v.get("creationDate") else None
        days = (now - cr.replace(tzinfo=None)).days if cr else 0
        title = v.get("jobTitle") or "—"
        if title in ("--","None","nan","None"): title = "Onbekend"
        co = (v.get("toCompany") or {}).get("name","—")
        ow = v.get("owner") or {}
        cons = f"{ow.get('firstName','')} {ow.get('lastName','')}".strip() or "—"
        ag = (v.get("agency") or {}).get("name","—")
        vrows.append({"title":title,"company":co,"consultant":cons,"agency":ag,"days":days,"matches":v.get("matchCount") or 0})

    vrows = sorted(vrows, key=lambda x: x["days"])

    # Summary KPIs
    k1,k2,k3 = st.columns(3)
    with k1:
        st.markdown(f"""<div class="card">
            <div class="card-label">Open Vacatures</div>
            <div class="card-val teal">{len(vrows)}</div>
            <div class="card-sub">actief uitstaand</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        avg_days = sum(v["days"] for v in vrows) / len(vrows) if vrows else 0
        st.markdown(f"""<div class="card">
            <div class="card-label">Gem. Openstaand</div>
            <div class="card-val white">{avg_days:.0f}<span style="font-size:1.5rem;"> dagen</span></div>
            <div class="card-sub">gemiddeld open</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        total_matches = sum(v["matches"] for v in vrows)
        st.markdown(f"""<div class="card">
            <div class="card-label">Totaal Kandidaten</div>
            <div class="card-val pink">{total_matches}</div>
            <div class="card-sub">voorgesteld</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    # Vacancy cards in 3 cols
    ca, cb, cc = st.columns(3)
    cols3 = [ca, cb, cc]
    for i, v in enumerate(vrows[:18]):
        dc = "#e92076" if v["days"]>60 else ("#f5a623" if v["days"]>30 else "#63ccca")
        with cols3[i%3]:
            st.markdown(f"""<div class="vac-card">
                <div class="vac-title">{v["title"]}</div>
                <div class="vac-meta">
                    <span>🏢 {v["company"]}</span>
                    <span>👤 {v["consultant"]}</span>
                    <span style="color:{dc}">⏱ {v["days"]}d</span>
                    <span>🎯 {v["matches"]}</span>
                </div>
            </div>""", unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center;padding:1.5rem 0 0.5rem;color:rgba(255,255,255,0.1);font-size:0.6rem;letter-spacing:3px;text-transform:uppercase;">
    Scherm {scr+1}/3 · {screen_names[scr]} · Wisselt elke 30s · Vernieuwd elke 30min
</div>""", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)