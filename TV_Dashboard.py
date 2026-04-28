import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import time as _time
from datetime import datetime, timedelta
import requests
import base64

st.set_page_config(
    page_title="Lincks Performance",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@300;400;500;600;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background: #0d0820; color: white; }
.stApp, [data-testid="stAppViewContainer"] { background: #0d0820; }
.block-container { padding: 0 !important; max-width: 100% !important; }
footer, #MainMenu, header, [data-testid="collapsedControl"] { display: none !important; }
section[data-testid="stSidebar"] { display: none !important; }

/* Grid bg */
.stApp::before {
    content: '';
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background-image: linear-gradient(rgba(233,32,118,0.04) 1px, transparent 1px),
                      linear-gradient(90deg, rgba(233,32,118,0.04) 1px, transparent 1px);
    background-size: 80px 80px;
    pointer-events: none; z-index: 0;
}

.wrap { position: relative; z-index: 1; padding: 1.5rem 2rem; }

/* Header */
.hdr { display: flex; align-items: center; justify-content: space-between; margin-bottom: 1.2rem; padding-bottom: 1rem; border-bottom: 1px solid rgba(255,255,255,0.06); }
.hdr-brand { display: flex; align-items: center; gap: 1rem; }
.hdr-tag { background: #e92076; color: white; font-size: 0.55rem; font-weight: 800; letter-spacing: 3px; padding: 0.3rem 0.7rem; border-radius: 4px; text-transform: uppercase; }
.clock { font-family: 'Syne', sans-serif; font-size: 2.8rem; font-weight: 800; color: #e92076; line-height: 1; letter-spacing: -1px; }
.clock-date { font-size: 0.65rem; color: rgba(255,255,255,0.3); letter-spacing: 2px; text-transform: uppercase; margin-top: 2px; text-align: right; }

/* Nav tabs */
.nav { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; align-items: center; flex-wrap: wrap; }
.nav-tab {
    padding: 0.5rem 1.2rem; border-radius: 8px; font-size: 0.75rem; font-weight: 600;
    letter-spacing: 1px; text-transform: uppercase; cursor: pointer;
    border: 1px solid rgba(255,255,255,0.1); color: rgba(255,255,255,0.4);
    background: rgba(255,255,255,0.03); transition: all 0.2s;
    text-decoration: none; display: inline-block;
}
.nav-tab.active { background: #e92076; border-color: #e92076; color: white; box-shadow: 0 4px 20px rgba(233,32,118,0.4); }
.nav-tab.exit { background: rgba(99,204,202,0.1); border-color: rgba(99,204,202,0.3); color: #63ccca; }

/* Cards */
.card {
    background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07);
    border-radius: 16px; padding: 1.4rem; position: relative; overflow: hidden;
}
.card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px; background: linear-gradient(90deg, transparent, rgba(233,32,118,0.4), transparent); }
.card-label { font-size: 0.6rem; font-weight: 700; color: rgba(255,255,255,0.3); text-transform: uppercase; letter-spacing: 3px; margin-bottom: 0.6rem; }
.card-val { font-family: 'Syne', sans-serif; font-size: 3rem; font-weight: 800; line-height: 1; }
.card-val.pink { color: #e92076; }
.card-val.teal { color: #63ccca; }
.card-sub { font-size: 0.72rem; color: rgba(255,255,255,0.3); margin-top: 0.4rem; }
.prog-track { height: 3px; background: rgba(255,255,255,0.06); border-radius: 3px; overflow: hidden; margin-top: 1rem; }
.prog-fill { height: 100%; border-radius: 3px; background: linear-gradient(90deg, #e92076, #ff6ab0); }

/* Funnel KPI */
.fkpi { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.07); border-radius: 14px; padding: 1.5rem 1rem; text-align: center; position: relative; overflow: hidden; }
.fkpi-val { font-family: 'Syne', sans-serif; font-size: 3.5rem; font-weight: 800; line-height: 1; }
.fkpi-label { font-size: 0.6rem; color: rgba(255,255,255,0.35); letter-spacing: 2px; text-transform: uppercase; margin-top: 0.5rem; }
.fkpi-bar { position: absolute; bottom: 0; left: 10%; right: 10%; height: 3px; border-radius: 2px; }

/* Vacancy carousel */
.vac-carousel { position: relative; overflow: hidden; width: 100%; }
.vac-track { display: flex; transition: transform 0.6s cubic-bezier(0.4,0,0.2,1); }
.vac-slide { min-width: 100%; padding: 0 1rem; }
.vac-card-big {
    background: linear-gradient(135deg, #1a0535 0%, #2d0a52 100%);
    border: 1px solid rgba(233,32,118,0.3);
    border-radius: 20px; padding: 2.5rem;
    position: relative; overflow: hidden;
    box-shadow: 0 20px 60px rgba(0,0,0,0.4);
}
.vac-card-big::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: linear-gradient(90deg, #e92076, #63ccca); }
.vac-card-big::after { content: ''; position: absolute; top: -50%; right: -20%; width: 400px; height: 400px; background: radial-gradient(circle, rgba(233,32,118,0.08) 0%, transparent 70%); pointer-events: none; }
.vac-num { font-size: 0.65rem; color: rgba(255,255,255,0.25); letter-spacing: 3px; text-transform: uppercase; margin-bottom: 0.5rem; }
.vac-title-big { font-family: 'Syne', sans-serif; font-size: 2.8rem; font-weight: 800; color: white; margin-bottom: 1rem; line-height: 1.1; }
.vac-company { font-size: 1.1rem; color: #63ccca; font-weight: 600; margin-bottom: 1.5rem; }
.vac-pills { display: flex; gap: 0.8rem; flex-wrap: wrap; margin-bottom: 2rem; }
.vac-pill { background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.1); border-radius: 20px; padding: 0.4rem 1rem; font-size: 0.8rem; color: rgba(255,255,255,0.6); }
.vac-pill.highlight { background: rgba(233,32,118,0.15); border-color: rgba(233,32,118,0.3); color: #ff9dc7; }
.vac-consultant { font-size: 0.75rem; color: rgba(255,255,255,0.35); }
.vac-dots { display: flex; justify-content: center; gap: 0.5rem; margin-top: 1.5rem; }
.vac-dot { width: 8px; height: 8px; border-radius: 50%; background: rgba(255,255,255,0.15); transition: all 0.3s; }
.vac-dot.active { background: #e92076; width: 24px; border-radius: 4px; box-shadow: 0 0 10px rgba(233,32,118,0.5); }

/* Small vac cards */
.vac-card-sm {
    background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.06);
    border-left: 3px solid #e92076; border-radius: 10px; padding: 0.9rem 1.1rem;
    margin-bottom: 0.6rem;
}
.vac-sm-title { font-size: 0.9rem; font-weight: 600; color: white; margin-bottom: 0.2rem; }
.vac-sm-meta { font-size: 0.7rem; color: rgba(255,255,255,0.35); }

/* Plotly */
.js-plotly-plot .plotly { background: transparent !important; }

/* Streamlit overrides */
.stSelectbox > div > div { background: rgba(255,255,255,0.04) !important; border: 1px solid rgba(255,255,255,0.1) !important; color: white !important; border-radius: 8px !important; font-size: 0.8rem !important; }
.stSelectbox label { color: rgba(255,255,255,0.3) !important; font-size: 0.65rem !important; letter-spacing: 2px !important; text-transform: uppercase !important; }
.stButton > button { background: rgba(255,255,255,0.05) !important; color: rgba(255,255,255,0.5) !important; border: 1px solid rgba(255,255,255,0.1) !important; border-radius: 8px !important; font-size: 0.72rem !important; letter-spacing: 1px !important; text-transform: uppercase !important; padding: 0.4rem 0.8rem !important; }
.stButton > button:hover { background: rgba(233,32,118,0.2) !important; border-color: rgba(233,32,118,0.4) !important; color: white !important; }
</style>
""", unsafe_allow_html=True)

# ── JS: live clock ────────────────────────────────────────────────────────────
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
def fetch_top_vacancies():
    """Fetch 10 most recent active vacancies to show as top vacatures."""
    all_vacs = []
    closed = ["Geplaatst","Verloren","Vervallen","Ingetrokken"]
    for pn in range(10):
        q = f"""{{
            crVacancyPage(pageable:{{page:{pn},size:100}}){{
                totalElements
                items{{
                    _id jobTitle vacancyNo creationDate statusDisplay
                    toCompany{{name}} owner{{firstName lastName}}
                    agency{{name}} matchCount
                    workLocation rawWorkLocation
                    toFunctionLevel1{{label}}
                }}
            }}
        }}"""
        data = run_query(q)
        if not data or not data.get("data"): break
        page = data.get("data",{}).get("crVacancyPage") or {}
        items = page.get("items",[])
        if not items: break
        for v in items:
            if v.get("statusDisplay") not in closed and v.get("jobTitle") and v.get("jobTitle") != "--":
                all_vacs.append(v)
        if len(all_vacs) >= 10: break
        _time.sleep(0.2)
    # Sort by creation date desc, take top 10
    all_vacs.sort(key=lambda x: x.get("creationDate",""), reverse=True)
    return all_vacs[:10]

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_funnel(month_start):
    stats = {"nieuwe_vacatures":0,"eerste_gesprek":0,"tweede_gesprek":0,"aanbod":0,"geplaatst":0}
    try:
        q1 = f"""{{ crMatchPage(qualifier:"creationDate > (NSCalendarDate) '{month_start} 00:00:00'",pageable:{{page:0,size:500}}){{
            items{{ _id statusInfo{{ displayName }} }} }} }}"""
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
if "manual" not in st.session_state: st.session_state["manual"] = False
if "vac_idx" not in st.session_state: st.session_state["vac_idx"] = 0
if "vac_last" not in st.session_state: st.session_state["vac_last"] = _time.time()

# Auto-switch between screen 0 and 1 every 60s (not when on screen 2)
if not st.session_state["manual"] and _time.time() - st.session_state["last_sw"] > 60:
    st.session_state["screen"] = 1 - st.session_state["screen"]  # toggle 0 ↔ 1
    st.session_state["last_sw"] = _time.time()

# Auto-advance vacancy carousel every 5s when on screen 2
if st.session_state["screen"] == 2 and _time.time() - st.session_state["vac_last"] > 5:
    st.session_state["vac_idx"] = (st.session_state["vac_idx"] + 1)
    st.session_state["vac_last"] = _time.time()

# Auto rerun every 5s
st.markdown("<script>setTimeout(function(){ window.location.reload(); }, 5000);</script>", unsafe_allow_html=True)

# ── Month ─────────────────────────────────────────────────────────────────────
all_months = sorted(df["month"].unique().tolist(), reverse=True)
now_m = nl_now().strftime("%Y-%m")
def_m = now_m if now_m in all_months else all_months[0]

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lincks_logo_fc-wit_def.png")
if os.path.exists(logo_path):
    with open(logo_path,"rb") as f: lb64 = base64.b64encode(f.read()).decode()
    logo_html = f'<img src="data:image/png;base64,{lb64}" style="height:42px;" />'
else:
    logo_html = '<span style="font-family:Syne,sans-serif;font-size:1.5rem;font-weight:800;">LINCKS</span>'

# ════════════════════════════════════════════════════════════════
# LAYOUT
# ════════════════════════════════════════════════════════════════
st.markdown('<div class="wrap">', unsafe_allow_html=True)

# Header
st.markdown(f"""
<div class="hdr">
    <div class="hdr-brand">
        {logo_html}
        <span class="hdr-tag">Performance</span>
    </div>
    <div style="text-align:right">
        <div class="clock" id="js-clock">--:--:--</div>
        <div class="clock-date" id="js-date">--</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Nav + controls
scr = st.session_state["screen"]
nc1,nc2,nc3,nc4,_sp,mc,ec = st.columns([0.9,0.9,1.1,0.6,2,1.5,1.5])
with nc1:
    if st.button("💰 Omzet", key="b0"):
        st.session_state["screen"]=0; st.session_state["last_sw"]=_time.time()
        st.session_state["manual"]=False; st.rerun()
with nc2:
    if st.button("📊 Pipeline", key="b1"):
        st.session_state["screen"]=1; st.session_state["last_sw"]=_time.time()
        st.session_state["manual"]=False; st.rerun()
with nc3:
    if st.button("⭐ Top Vacatures", key="b2"):
        st.session_state["screen"]=2; st.session_state["manual"]=True
        st.session_state["vac_idx"]=0; st.session_state["vac_last"]=_time.time(); st.rerun()
with nc4:
    if st.button("🔄", key="br"):
        st.cache_data.clear()
        for k in ["tv_done","tv_inv"]: st.session_state.pop(k,None)
        st.rerun()
with mc:
    sel_m = st.selectbox("Maand", all_months, index=all_months.index(def_m), label_visibility="collapsed")
with ec:
    st.markdown("""<a href="https://www.lincks.nl/vacatures" target="_blank"
       style="display:block;text-align:center;background:rgba(99,204,202,0.08);
       border:1px solid rgba(99,204,202,0.2);border-radius:8px;padding:0.48rem;
       color:#63ccca;text-decoration:none;font-size:0.7rem;letter-spacing:1px;
       text-transform:uppercase;margin-top:2px;">🌐 Vacaturesite ↗</a>""", unsafe_allow_html=True)

# Active indicator
screen_names = ["💰 Omzet","📊 Pipeline","⭐ Top Vacatures"]
status_text = "🔁 Automatisch wisselend" if not st.session_state["manual"] else "📌 Handmatig vergrendeld op Top Vacatures"
st.markdown(f"""
<div style="display:flex;align-items:center;gap:1rem;margin:0.5rem 0 1.2rem 0;padding-bottom:0.8rem;border-bottom:1px solid rgba(255,255,255,0.05);">
    <div style="font-size:0.65rem;color:rgba(255,255,255,0.25);letter-spacing:2px;text-transform:uppercase;">{screen_names[scr]}</div>
    <div style="font-size:0.6rem;color:rgba(255,255,255,0.2);letter-spacing:1px;">{status_text}</div>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 0 — OMZET
# ════════════════════════════════════════════════════════════════
if scr == 0:
    filtered  = df[df["month"] == sel_m]
    total_rev = filtered.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
    pct       = min(total_rev / COMPANY_TARGET * 100, 100)
    remaining = max(COMPANY_TARGET - total_rev, 0)
    excl = filtered[~filtered["consultant"].isin(["Mireille Prooi","Onbekend"])]
    top_c = excl.groupby("consultant")["revenue"].sum().idxmax() if not excl.empty else "—"
    top_v = excl.groupby("consultant")["revenue"].sum().max() if not excl.empty else 0

    k1,k2,k3 = st.columns(3)
    with k1:
        st.markdown(f"""<div class="card">
            <div class="card-label">Maandomzet {sel_m}</div>
            <div class="card-val pink">€{total_rev:,.0f}</div>
            <div class="card-sub">doel €{COMPANY_TARGET:,.0f}</div>
            <div class="prog-track"><div class="prog-fill" style="width:{pct:.1f}%"></div></div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""<div class="card">
            <div class="card-label">Top Recruiter</div>
            <div class="card-val teal" style="font-size:1.8rem;margin-top:0.3rem;">{top_c}</div>
            <div class="card-sub">€{top_v:,.0f} deze maand</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div class="card">
            <div class="card-label">Target Voortgang</div>
            <div class="card-val {"teal" if pct>=100 else "pink"}">{pct:.0f}%</div>
            <div class="card-sub">€{remaining:,.0f} te gaan</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    col_donut, col_bar = st.columns([1, 2.2])
    with col_donut:
        fig = go.Figure(go.Pie(
            values=[total_rev, remaining], labels=["Behaald","Resterend"],
            hole=0.78, sort=False, textinfo="none",
            marker_colors=["#e92076","rgba(255,255,255,0.04)"],
            marker=dict(line=dict(width=0)),
        ))
        fig.add_annotation(text=f"<b>{pct:.0f}%</b>", x=0.5, y=0.60,
            font=dict(size=64, color="#e92076", family="Syne"), showarrow=False)
        fig.add_annotation(text=f"€{total_rev:,.0f}", x=0.5, y=0.42,
            font=dict(size=18, color="rgba(255,255,255,0.7)"), showarrow=False)
        fig.add_annotation(text=f"van €{COMPANY_TARGET:,.0f}", x=0.5, y=0.27,
            font=dict(size=12, color="rgba(255,255,255,0.3)"), showarrow=False)
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False, margin=dict(l=10,r=10,t=10,b=10), height=380)
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
        ))
        fig2.add_trace(go.Bar(
            x=rc["rest"], y=rc["consultant"], orientation="h", name="Resterend",
            marker_color="rgba(255,255,255,0.03)",
            marker_line_color="rgba(255,255,255,0.08)", marker_line_width=1,
        ))
        fig2.update_layout(
            barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.5)",
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickprefix="€", tickfont=dict(size=11), zeroline=False),
            yaxis=dict(tickfont=dict(size=14, family="DM Sans"), gridcolor="rgba(0,0,0,0)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
                        font=dict(color="rgba(255,255,255,0.4)", size=11)),
            margin=dict(l=0,r=10,t=30,b=0), height=380, bargap=0.22,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 1 — PIPELINE
# ════════════════════════════════════════════════════════════════
elif scr == 1:
    month_start = f"{sel_m}-01"
    with st.spinner(""):
        stats = fetch_funnel(month_start)

    kpi_items = [
        ("Nieuwe Vacatures", stats["nieuwe_vacatures"], "#63ccca", "#63ccca"),
        ("1e Gesprek Klant",  stats["eerste_gesprek"],  "#e92076", "#e92076"),
        ("2e Gesprek Klant",  stats["tweede_gesprek"],  "#f5a623", "#f5a623"),
        ("Heeft Aanbod",      stats["aanbod"],          "#a78bfa", "#a78bfa"),
        ("Geplaatst ✓",       stats["geplaatst"],       "#00e5a0", "#00e5a0"),
    ]
    cols = st.columns(5)
    for col, (lbl, val, clr, bar_clr) in zip(cols, kpi_items):
        with col:
            st.markdown(f"""<div class="fkpi">
                <div class="fkpi-val" style="color:{clr}">{val}</div>
                <div class="fkpi-label">{lbl}</div>
                <div class="fkpi-bar" style="background:{bar_clr}"></div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

    col_funnel, col_trend = st.columns([1.2, 1])
    with col_funnel:
        labels  = ["Nieuwe Vacatures","1e Gesprek","2e Gesprek","Aanbod","Geplaatst"]
        values  = [stats["nieuwe_vacatures"], stats["eerste_gesprek"],
                   stats["tweede_gesprek"], stats["aanbod"], stats["geplaatst"]]
        colors  = ["#63ccca","#e92076","#f5a623","#a78bfa","#00e5a0"]
        fig3 = go.Figure(go.Funnel(
            y=labels, x=values, textinfo="value+percent initial",
            marker=dict(color=colors, line=dict(width=0)),
            connector=dict(line=dict(color="rgba(255,255,255,0.06)", width=2)),
            textfont=dict(size=16, color="white", family="DM Sans"),
        ))
        fig3.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.6)",
            margin=dict(l=180,r=60,t=20,b=20), height=340,
            yaxis=dict(tickfont=dict(size=14, family="DM Sans")),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_trend:
        # Month-over-month revenue comparison
        filtered_now = df[df["month"] == sel_m]
        now_total = filtered_now.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
        idx = all_months.index(sel_m)
        prev_total = 0
        if idx + 1 < len(all_months):
            prev_m = all_months[idx+1]
            prev_total = df[df["month"]==prev_m].drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()

        delta = now_total - prev_total
        delta_pct = (delta/prev_total*100) if prev_total > 0 else 0
        arrow = "↑" if delta >= 0 else "↓"
        clr = "#00e5a0" if delta >= 0 else "#e92076"

        st.markdown(f"""
        <div class="card" style="margin-bottom:1rem;">
            <div class="card-label">Omzet vs vorige maand</div>
            <div style="font-family:Syne,sans-serif;font-size:2.2rem;font-weight:800;color:{clr};margin-top:0.3rem;">
                {arrow} €{abs(delta):,.0f}
            </div>
            <div class="card-sub">{delta_pct:+.1f}% ten opzichte van vorige maand</div>
        </div>
        <div class="card">
            <div class="card-label">Maandomzet {sel_m}</div>
            <div class="card-val pink" style="font-size:2rem;">€{now_total:,.0f}</div>
            <div class="card-sub">{min(now_total/COMPANY_TARGET*100,100):.0f}% van maandtarget €{COMPANY_TARGET:,.0f}</div>
            <div class="prog-track"><div class="prog-fill" style="width:{min(now_total/COMPANY_TARGET*100,100):.1f}%"></div></div>
        </div>
        """, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# SCREEN 2 — TOP VACATURES (carousel)
# ════════════════════════════════════════════════════════════════
elif scr == 2:
    with st.spinner(""):
        top_vacs = fetch_top_vacancies()

    if not top_vacs:
        st.info("Geen vacatures gevonden.")
    else:
        idx = st.session_state["vac_idx"] % len(top_vacs)
        v = top_vacs[idx]

        title = v.get("jobTitle") or "—"
        company = (v.get("toCompany") or {}).get("name","—")
        ow = v.get("owner") or {}
        consultant = f"{ow.get('firstName','')} {ow.get('lastName','')}".strip() or "—"
        agency = (v.get("agency") or {}).get("name","—")
        matches = v.get("matchCount") or 0
        created = pd.to_datetime(v.get("creationDate")) if v.get("creationDate") else None
        days_open = (nl_now() - created.replace(tzinfo=None)).days if created else 0
        func = (v.get("toFunctionLevel1") or {}).get("label","")
        location = v.get("workLocation") or v.get("rawWorkLocation") or "Nederland"
        status = v.get("statusDisplay","")
        vac_no = v.get("vacancyNo","")

        # Dots
        dots = "".join([f'<div class="vac-dot {"active" if i==idx else ""}"></div>' for i in range(len(top_vacs))])

        # Pills
        pills = ""
        if func and func != "Dossier": pills += f'<span class="vac-pill">{func}</span>'
        if location: pills += f'<span class="vac-pill">📍 {location}</span>'
        if matches: pills += f'<span class="vac-pill highlight">🎯 {matches} kandidaten</span>'
        if days_open: pills += f'<span class="vac-pill">⏱ {days_open} dagen open</span>'

        col_main, col_list = st.columns([1.6, 1])

        with col_main:
            st.markdown(f"""
            <div class="vac-card-big">
                <div class="vac-num">Vacature {vac_no} · {status}</div>
                <div class="vac-title-big">{title}</div>
                <div class="vac-company">🏢 {company}</div>
                <div class="vac-pills">{pills}</div>
                <div class="vac-consultant">👤 Recruiter: {consultant} · {agency}</div>
            </div>
            <div class="vac-dots">{dots}</div>
            """, unsafe_allow_html=True)

            # Prev/Next buttons
            btn_c1, btn_c2, btn_c3 = st.columns([1,1,4])
            with btn_c1:
                if st.button("← Vorige", key="vprev"):
                    st.session_state["vac_idx"] = (idx - 1) % len(top_vacs)
                    st.session_state["vac_last"] = _time.time()
                    st.rerun()
            with btn_c2:
                if st.button("Volgende →", key="vnext"):
                    st.session_state["vac_idx"] = (idx + 1) % len(top_vacs)
                    st.session_state["vac_last"] = _time.time()
                    st.rerun()

        with col_list:
            st.markdown('<div style="padding-left:0.5rem">', unsafe_allow_html=True)
            st.markdown('<div style="font-size:0.6rem;color:rgba(255,255,255,0.25);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.8rem;">Alle top vacatures</div>', unsafe_allow_html=True)
            for i, vv in enumerate(top_vacs):
                t2 = vv.get("jobTitle","—")
                c2 = (vv.get("toCompany") or {}).get("name","—")
                active_style = "border-left-color:#63ccca;background:rgba(99,204,202,0.05);" if i==idx else ""
                st.markdown(f"""<div class="vac-card-sm" style="{active_style}cursor:pointer;">
                    <div class="vac-sm-title">{t2}</div>
                    <div class="vac-sm-meta">🏢 {c2}</div>
                </div>""", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

# Footer
screen_names_footer = ["💰 Omzet","📊 Pipeline","⭐ Top Vacatures"]
auto_txt = "wisselt automatisch elke 60s" if not st.session_state["manual"] else "handmatig · klik ander scherm om te hervatten"
st.markdown(f"""
<div style="text-align:center;padding:1.5rem 0 0.5rem;color:rgba(255,255,255,0.08);font-size:0.6rem;letter-spacing:3px;text-transform:uppercase;">
    {screen_names_footer[scr]} · {auto_txt}
</div>""", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)