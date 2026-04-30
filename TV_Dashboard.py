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

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=Inter:wght@300;400;500;600;700&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
html,body,[class*="css"]{font-family:'Inter',sans-serif;background:#080614;color:white;}
.stApp,[data-testid="stAppViewContainer"]{background:#080614;}
.block-container{padding:0!important;max-width:100%!important;}
footer,#MainMenu,header,[data-testid="collapsedControl"]{display:none!important;}
section[data-testid="stSidebar"]{display:none!important;}
.stApp::before{content:'';position:fixed;top:0;left:0;right:0;bottom:0;
  background-image:linear-gradient(rgba(233,32,118,0.03) 1px,transparent 1px),
  linear-gradient(90deg,rgba(233,32,118,0.03) 1px,transparent 1px);
  background-size:60px 60px;pointer-events:none;z-index:0;}
.wrap{position:relative;z-index:1;padding:1.8rem 2.5rem 1rem;}
.hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:1.5rem;}
.hdr-left{display:flex;align-items:center;gap:1.2rem;}
.hdr-badge{background:#e92076;color:white;font-size:0.55rem;font-weight:700;letter-spacing:3px;padding:0.25rem 0.7rem;border-radius:3px;text-transform:uppercase;}
.clock{font-family:'Syne',sans-serif;font-size:3.2rem;font-weight:800;color:#e92076;line-height:1;letter-spacing:-2px;}
.clock-date{font-size:0.62rem;color:rgba(255,255,255,0.25);letter-spacing:3px;text-transform:uppercase;margin-top:3px;text-align:right;}
.divider{height:1px;background:rgba(255,255,255,0.06);margin-bottom:1.2rem;}
.card{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);border-radius:14px;padding:1.5rem;position:relative;overflow:hidden;}
.card-top{position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#e92076,transparent);}
.card-label{font-size:0.58rem;font-weight:600;color:rgba(255,255,255,0.25);text-transform:uppercase;letter-spacing:3px;margin-bottom:0.7rem;}
.card-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.pink{color:#e92076;}.teal{color:#00d4c8;}.green{color:#00e5a0;}
.card-sub{font-size:0.7rem;color:rgba(255,255,255,0.25);margin-top:0.5rem;}
.prog{height:2px;background:rgba(255,255,255,0.06);border-radius:2px;margin-top:1rem;overflow:hidden;}
.prog-fill{height:100%;background:linear-gradient(90deg,#e92076,#ff6ab0);border-radius:2px;}
.fkpi{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:1.8rem 1rem;text-align:center;}
.fkpi-val{font-family:'Syne',sans-serif;font-size:3.8rem;font-weight:800;line-height:1;}
.fkpi-lbl{font-size:0.58rem;color:rgba(255,255,255,0.3);letter-spacing:2px;text-transform:uppercase;margin-top:0.6rem;}
.vac-big{background:linear-gradient(145deg,#150330 0%,#220844 60%,#1a0535 100%);border:1px solid rgba(233,32,118,0.2);border-radius:18px;padding:2.5rem 2.8rem;position:relative;overflow:hidden;min-height:320px;}
.vac-big::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#e92076 0%,#63ccca 50%,transparent 100%);}
.vac-meta-tag{font-size:0.6rem;color:rgba(255,255,255,0.25);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.8rem;}
.vac-title{font-family:'Syne',sans-serif;font-size:2.6rem;font-weight:800;color:white;line-height:1.1;margin-bottom:0.8rem;}
.vac-company{font-size:1rem;color:#00d4c8;font-weight:600;margin-bottom:1.5rem;}
.pills{display:flex;gap:0.6rem;flex-wrap:wrap;margin-bottom:1.5rem;}
.pill{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);border-radius:20px;padding:0.35rem 0.9rem;font-size:0.75rem;color:rgba(255,255,255,0.55);}
.pill.hot{background:rgba(233,32,118,0.12);border-color:rgba(233,32,118,0.25);color:#ff9dc7;}
.vac-recruiter{font-size:0.72rem;color:rgba(255,255,255,0.3);}
.dots{display:flex;gap:0.4rem;justify-content:center;margin-top:1.2rem;}
.dot{width:6px;height:6px;border-radius:50%;background:rgba(255,255,255,0.12);}
.dot.on{background:#e92076;width:20px;border-radius:3px;}
.vac-sm{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05);border-left:2px solid rgba(233,32,118,0.4);border-radius:8px;padding:0.8rem 1rem;margin-bottom:0.5rem;}
.vac-sm.cur{border-left-color:#00d4c8;background:rgba(0,212,200,0.04);}
.vac-sm-t{font-size:0.85rem;font-weight:600;color:white;margin-bottom:0.15rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.vac-sm-c{font-size:0.68rem;color:rgba(255,255,255,0.3);}
.stSelectbox>div>div{background:rgba(255,255,255,0.04)!important;border:1px solid rgba(255,255,255,0.08)!important;color:white!important;border-radius:8px!important;font-size:0.8rem!important;}
.stButton>button{background:rgba(255,255,255,0.04)!important;color:rgba(255,255,255,0.45)!important;border:1px solid rgba(255,255,255,0.08)!important;border-radius:8px!important;font-size:0.7rem!important;letter-spacing:1.5px!important;text-transform:uppercase!important;font-weight:600!important;padding:0.45rem 1rem!important;}
.stButton>button:hover{background:rgba(233,32,118,0.15)!important;border-color:rgba(233,32,118,0.35)!important;color:white!important;}
</style>
<meta http-equiv="refresh" content="7200">
""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID     = "4db4f54a9c90230221da81f085ef3bd5.apps.carerix.io"
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
TOKEN_URL     = "https://id-s3.carerix.io/auth/realms/lincks/protocol/openid-connect/token"
API_URL       = "https://api.carerix.io/graphql/v1/graphql"
DATA_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

TARGETS = {
    "Mireille Prooi":0,"Renate Leeuwenstein":35000,"Annemieke Bakker":35000,
    "Arjan Huisman":35000,"Dico Cabout":31250,"Sharon van de Haven":31250,
    "Sharon Kruijssen":31250,"Rick de Wit":15000,"Rick Wit":15000,
    "Esther Moerman":16667,"Birgit Lucas":35000,"Nina van Harten":15000,"Nina Harten":15000,
}
DEFAULT_TARGET = 31250
COMPANY_TARGET = 250000
MARGINS = {
    "Bakker Barendrecht":(0.294693758+0.303208773)/2,
    "Bakker Barendrecht ":(0.294693758+0.303208773)/2,
    "Nsecure BV":(0.424133148+0.407688238)/2,
    "Still Intern Transport":0.3885738,
    "Beveco Gebouwautomatisering B.V.":1.0,
}

def nl_now(): return datetime.utcnow()+timedelta(hours=2)
CURRENT_MONTH = nl_now().strftime("%Y-%m")
PREV_MONTH    = (nl_now().replace(day=1)-timedelta(days=1)).strftime("%Y-%m")

def get_token():
    r=requests.post(TOKEN_URL,data={"grant_type":"client_credentials","client_id":CLIENT_ID,"client_secret":CLIENT_SECRET})
    return r.json()["access_token"]

def run_query(q):
    t=get_token()
    r=requests.post(API_URL,json={"query":q},headers={"Authorization":f"Bearer {t}"},timeout=60)
    return r.json()

def get_last_sync():
    p=os.path.join(DATA_DIR,"sync_meta.parquet")
    if os.path.exists(p):
        try: return str(pd.read_parquet(p)["last_incremental_sync"].iloc[0])[:10]
        except: pass
    return "2026-04-02"

# ── Cached data functions — TTL 2 hours ──────────────────────────────────────
@st.cache_data(ttl=7200, show_spinner=False)
def load_invoice_df():
    """Load parquet, filter to current+prev month only, build df. Cached 2h."""
    users_df = pd.read_parquet(os.path.join(DATA_DIR,"users.parquet"))
    users    = dict(zip(users_df["user_id"], users_df["full_name"]))
    pl       = pd.read_parquet(os.path.join(DATA_DIR,"placements.parquet"))
    inv_raw  = pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet"))

    # Incremental fetch for new invoices
    last_sync = get_last_sync()
    today = nl_now().strftime("%Y-%m-%d")
    if last_sync < today and CLIENT_SECRET:
        try:
            probe = run_query("{ crInvoicePage(pageable:{page:0,size:1}){ totalElements } }")
            total = (probe.get("data",{}).get("crInvoicePage") or {}).get("totalElements",0)
            if total:
                lp = (total-1)//100
                for pn in range(max(0,lp-1), lp+1):
                    q = f"""{{ crInvoicePage(pageable:{{page:{pn},size:100}}){{
                        items{{ _id invoiceID valueDate date total statusDisplay companyName
                               toCompany{{name}} toJob{{_id}} agency{{name}} toInvoice{{_id valueDate date}} }}
                    }} }}"""
                    data = run_query(q)
                    items = (data.get("data",{}).get("crInvoicePage") or {}).get("items",[])
                    rows = []
                    for i in items:
                        vd=str(i.get("valueDate") or "")[:10]
                        d=str(i.get("date") or "")[:10]
                        ds=max(vd,d) if vd and d else (vd or d)
                        if ds and ds > last_sync:
                            og=i.get("toInvoice") or {}
                            co=(i.get("toCompany") or {}).get("name") or i.get("companyName") or "Unknown"
                            ag=(i.get("agency") or {}).get("name","Unknown")
                            jb=i.get("toJob") or {}
                            rows.append({"invoice_id":i.get("invoiceID"),"internal_id":str(i["_id"]),
                                "value_date":i.get("valueDate"),"date":i.get("date"),
                                "original_date":og.get("valueDate") or og.get("date"),
                                "total":float(i.get("total") or 0),"status":i.get("statusDisplay"),
                                "company":co,"agency":ag,"job_id":str(jb.get("_id","")) if jb else ""})
                    if rows:
                        new_df = pd.DataFrame(rows)
                        inv_raw = pd.concat([inv_raw,new_df],ignore_index=True).drop_duplicates(subset=["internal_id"],keep="last")
                    _time.sleep(0.3)
        except Exception as e:
            print(f"[FETCH] {e}")

    # ── Filter to current + prev month BEFORE building df ────────────────────
    # Parse best date first
    inv_raw["_vd"] = pd.to_datetime(inv_raw.get("value_date", inv_raw.get("date", "")), errors="coerce")
    inv_raw["_d"]  = pd.to_datetime(inv_raw.get("date", ""), errors="coerce")
    inv_raw["_best"] = inv_raw[["_vd","_d"]].max(axis=1)
    inv_raw["_month"] = inv_raw["_best"].dt.strftime("%Y-%m")

    # Keep only current and previous month — huge speed win
    inv_filtered = inv_raw[inv_raw["_month"].isin([CURRENT_MONTH, PREV_MONTH])].copy()

    # Build df
    job_map = {str(p["placement_id"]): p for _, p in pl.iterrows()}
    rows = []
    for _, i in inv_filtered.iterrows():
        amt = float(i.get("total") or 0)
        if amt == 0: continue
        if str(i.get("status") or "") != "Verzonden": continue
        vd = str(i.get("value_date") or "")[:10]
        d  = str(i.get("date") or "")[:10]
        og = str(i.get("original_date") or "")[:10]
        if amt < 0 and og and og not in ("None","nan",""): ds = og
        else: ds = max(vd,d) if vd and d else (vd or d)
        ds = ds.replace("None","").replace("nan","").strip()
        if not ds: continue
        try: date = pd.to_datetime(ds)
        except: continue
        month = date.strftime("%Y-%m")
        if month not in [CURRENT_MONTH, PREV_MONTH]: continue
        co = str(i.get("company") or "Unknown")
        ag = str(i.get("agency") or "Unknown")
        jid = str(i.get("job_id") or "")
        adj = amt * MARGINS.get(co,1.0) if ag == "Lincks Personeel B.V." else amt
        cons = []
        if jid and jid in job_map:
            p = job_map[jid]
            fu = list(set([str(p[k]) for k in ["fase_6525","fase_6526","fase_6527","fase_6528","fase_6538"]
                          if k in p.index and str(p[k]) not in ("","nan","None")]))
            cons = [users.get(u, f"User {u}") for u in fu]
        if not cons: cons = ["Mireille Prooi"]
        rp = adj / len(cons)
        for c in cons:
            rows.append({"consultant":c,"amount":adj,"revenue":rp,"date":date,"month":month})
    return pd.DataFrame(rows)

@st.cache_data(ttl=7200, show_spinner=False)
def load_funnel():
    """Fetch funnel stats for current month. Cached 2h."""
    ms = f"{CURRENT_MONTH}-01"
    s  = {"nieuwe_vacatures":0,"eerste_gesprek":0,"tweede_gesprek":0,"aanbod":0,"geplaatst":0}
    try:
        q1 = f"""{{ crMatchPage(qualifier:"creationDate > (NSCalendarDate) '{ms} 00:00:00'",pageable:{{page:0,size:500}}){{
            items{{ statusInfo{{ displayName }} }} }} }}"""
        for m in (run_query(q1).get("data",{}).get("crMatchPage") or {}).get("items",[]):
            sn = str((m.get("statusInfo") or {}).get("displayName") or "")
            if "4.1" in sn: s["eerste_gesprek"] += 1
            elif "4.2" in sn: s["tweede_gesprek"] += 1
            elif "5.0" in sn: s["aanbod"] += 1
        q2 = f"""{{ crJobPage(qualifier:"creationDate > (NSCalendarDate) '{ms} 00:00:00'",pageable:{{page:0,size:1}}){{ totalElements }} }}"""
        s["geplaatst"] = (run_query(q2).get("data",{}).get("crJobPage") or {}).get("totalElements",0)
        q3 = f"""{{ crVacancyPage(qualifier:"creationDate > (NSCalendarDate) '{ms} 00:00:00'",pageable:{{page:0,size:1}}){{ totalElements }} }}"""
        s["nieuwe_vacatures"] = (run_query(q3).get("data",{}).get("crVacancyPage") or {}).get("totalElements",0)
    except Exception as e: print(f"[FUNNEL] {e}")
    return s

@st.cache_data(ttl=7200, show_spinner=False)
def load_vacancies():
    """Fetch 10 recent active vacancies. Cached 2h."""
    closed = ["Geplaatst","Verloren","Vervallen","Ingetrokken"]
    result = []
    for pn in range(15):
        q = f"""{{ crVacancyPage(pageable:{{page:{pn},size:100}}){{
            totalElements items{{
                _id jobTitle vacancyNo creationDate statusDisplay
                toCompany{{name}} owner{{firstName lastName}} agency{{name}}
                matchCount workLocation rawWorkLocation toFunctionLevel1{{label}}
            }}
        }} }}"""
        data = run_query(q)
        if not data or not data.get("data"): break
        page = data.get("data",{}).get("crVacancyPage") or {}
        items = page.get("items",[])
        if not items: break
        for v in items:
            if v.get("statusDisplay") not in closed and v.get("jobTitle") and v.get("jobTitle") not in ("--","None"):
                result.append(v)
        if len(result) >= 10: break
        _time.sleep(0.2)
    result.sort(key=lambda x: x.get("creationDate",""), reverse=True)
    return result[:10]

# ── Load all data once ────────────────────────────────────────────────────────
with st.spinner(""):
    try:
        df      = load_invoice_df()
        stats   = load_funnel()
        vacs    = load_vacancies()
        data_ok = True
    except Exception as e:
        st.error(f"Error: {e}"); data_ok = False

if not data_ok or df.empty:
    st.warning("Geen data."); st.stop()

# Pre-compute current month data once
df_cur  = df[df["month"] == CURRENT_MONTH]
df_prev = df[df["month"] == PREV_MONTH]
tot     = df_cur.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
prev_tot= df_prev.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
pct     = min(tot / COMPANY_TARGET * 100, 100)
rem     = max(COMPANY_TARGET - tot, 0)
delta   = tot - prev_tot
dpc     = (delta/prev_tot*100) if prev_tot > 0 else 0
excl    = df_cur[~df_cur["consultant"].isin(["Mireille Prooi","Onbekend"])]
top_c   = excl.groupby("consultant")["revenue"].sum().idxmax() if not excl.empty else "—"
top_v   = excl.groupby("consultant")["revenue"].sum().max() if not excl.empty else 0

# ── Screen state ──────────────────────────────────────────────────────────────
if "screen" not in st.session_state: st.session_state["screen"] = 0
if "vac_idx" not in st.session_state: st.session_state["vac_idx"] = 0
if "manual" not in st.session_state: st.session_state["manual"] = False

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lincks_logo_fc-wit_def.png")
if os.path.exists(logo_path):
    with open(logo_path,"rb") as f: lb64 = base64.b64encode(f.read()).decode()
    logo_html = f'<img src="data:image/png;base64,{lb64}" style="height:40px;" />'
else:
    logo_html = '<span style="font-family:Syne,sans-serif;font-size:1.4rem;font-weight:800;">LINCKS</span>'

# ════════════════════════════════════════════════════════════════
# STATIC HEADER — rendered once, never changes
# ════════════════════════════════════════════════════════════════
st.markdown(f"""
<div class="wrap">
<div class="hdr">
    <div class="hdr-left">
        {logo_html}
        <span class="hdr-badge">Live · {CURRENT_MONTH}</span>
    </div>
    <div style="text-align:right">
        <div class="clock" id="clk">--:--:--</div>
        <div class="clock-date" id="clkd">--</div>
    </div>
</div>
<div class="divider"></div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# NAV — fragment so only nav rerenders on click
# ════════════════════════════════════════════════════════════════
@st.fragment
def render_nav():
    n1,n2,n3,n4,_sp,ec = st.columns([0.8,0.8,1.1,0.55,3.2,1.4])
    with n1:
        if st.button("💰 Omzet", key="b0"):
            st.session_state.update({"screen":0,"manual":False})
            st.rerun(scope="app")
    with n2:
        if st.button("📊 Pipeline", key="b1"):
            st.session_state.update({"screen":1,"manual":False})
            st.rerun(scope="app")
    with n3:
        if st.button("⭐ Top Vacatures", key="b2"):
            st.session_state.update({"screen":2,"manual":True,"vac_idx":0})
            st.rerun(scope="app")
    with n4:
        if st.button("↺", key="br"):
            st.cache_data.clear()
            st.rerun(scope="app")
    with ec:
        st.markdown("""<a href="https://www.lincks.nl/vacatures" target="_blank"
           style="display:flex;align-items:center;justify-content:center;
           background:rgba(0,212,200,0.07);border:1px solid rgba(0,212,200,0.18);
           border-radius:8px;padding:0.45rem 0.8rem;color:#00d4c8;text-decoration:none;
           font-size:0.68rem;letter-spacing:1px;text-transform:uppercase;font-weight:600;">
           🌐 Vacaturesite</a>""", unsafe_allow_html=True)
    st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)

render_nav()

# ════════════════════════════════════════════════════════════════
# SCREEN CONTENT — fragment so data doesn't reload on nav click
# ════════════════════════════════════════════════════════════════
@st.fragment
def render_screen():
    scr = st.session_state["screen"]

    # ── SCREEN 0 — OMZET ──
    if scr == 0:
        k1,k2,k3 = st.columns(3)
        with k1:
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Maandomzet {CURRENT_MONTH}</div>
                <div class="card-val pink">€{tot:,.0f}</div>
                <div class="card-sub">doel €{COMPANY_TARGET:,.0f} · nog €{rem:,.0f}</div>
                <div class="prog"><div class="prog-fill" style="width:{pct:.1f}%"></div></div>
            </div>""", unsafe_allow_html=True)
        with k2:
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Top Recruiter</div>
                <div class="card-val teal" style="font-size:1.9rem;padding-top:0.3rem">{top_c}</div>
                <div class="card-sub">€{top_v:,.0f} deze maand</div>
            </div>""", unsafe_allow_html=True)
        with k3:
            clr = "green" if pct>=100 else "pink"
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Target Voortgang</div>
                <div class="card-val {clr}">{pct:.0f}%</div>
                <div class="card-sub">van €{COMPANY_TARGET:,.0f} maandtarget</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        cd,cb = st.columns([1,2.2])

        with cd:
            fig = go.Figure(go.Pie(values=[tot,rem], labels=["Behaald","Resterend"],
                hole=0.8, sort=False, textinfo="none",
                marker_colors=["#e92076","rgba(255,255,255,0.04)"],
                marker=dict(line=dict(width=0))))
            fig.add_annotation(text=f"<b>{pct:.0f}%</b>", x=0.5, y=0.58,
                font=dict(size=60,color="#e92076",family="Syne"), showarrow=False)
            fig.add_annotation(text=f"€{tot:,.0f}", x=0.5, y=0.40,
                font=dict(size=17,color="rgba(255,255,255,0.65)"), showarrow=False)
            fig.add_annotation(text=f"van €{COMPANY_TARGET:,.0f}", x=0.5, y=0.27,
                font=dict(size=11,color="rgba(255,255,255,0.25)"), showarrow=False)
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False, margin=dict(l=10,r=10,t=10,b=10), height=360)
            st.plotly_chart(fig, use_container_width=True)

        with cb:
            def get_t(n):
                if n in TARGETS: return TARGETS[n]
                last = n.split()[-1] if n else ""
                for k,v in TARGETS.items():
                    if k.split()[-1] == last: return v
                return DEFAULT_TARGET
            rc = excl.groupby("consultant")["revenue"].sum().reset_index()
            rc["t"]   = rc["consultant"].apply(get_t)
            rc["p"]   = (rc["revenue"]/rc["t"]*100).round(1)
            rc["r"]   = (rc["t"]-rc["revenue"]).clip(lower=0)
            rc = rc.sort_values("revenue", ascending=True)
            rc["c"]   = rc["p"].apply(lambda x:"#00e5a0" if x>=100 else("#f5a623" if x>=80 else"#e92076"))
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(x=rc["revenue"], y=rc["consultant"], orientation="h", name="Behaald",
                marker_color=rc["c"], marker_line_width=0,
                text=rc.apply(lambda r:f"  €{r['revenue']:,.0f}  ·  {r['p']:.0f}%",axis=1),
                textposition="inside", insidetextanchor="start",
                textfont=dict(color="white",size=12,family="Inter")))
            fig2.add_trace(go.Bar(x=rc["r"], y=rc["consultant"], orientation="h", name="Resterend",
                marker_color="rgba(255,255,255,0.03)",
                marker_line_color="rgba(255,255,255,0.06)", marker_line_width=1))
            fig2.update_layout(barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font_color="rgba(255,255,255,0.45)",
                xaxis=dict(gridcolor="rgba(255,255,255,0.04)",tickprefix="€",tickfont=dict(size=10),zeroline=False),
                yaxis=dict(tickfont=dict(size=13,family="Inter"),gridcolor="rgba(0,0,0,0)"),
                legend=dict(orientation="h",yanchor="bottom",y=1.01,xanchor="right",x=1,
                    font=dict(color="rgba(255,255,255,0.3)",size=10)),
                margin=dict(l=0,r=10,t=30,b=0), height=360, bargap=0.2)
            st.plotly_chart(fig2, use_container_width=True)

    # ── SCREEN 1 — PIPELINE ──
    elif scr == 1:
        kpis = [
            ("Nieuwe Vacatures", stats["nieuwe_vacatures"], "#00d4c8"),
            ("1e Gesprek Klant",  stats["eerste_gesprek"],  "#e92076"),
            ("2e Gesprek Klant",  stats["tweede_gesprek"],  "#f5a623"),
            ("Heeft Aanbod",      stats["aanbod"],          "#a78bfa"),
            ("Geplaatst ✓",       stats["geplaatst"],       "#00e5a0"),
        ]
        cols = st.columns(5)
        for col,(lbl,val,clr) in zip(cols,kpis):
            with col:
                st.markdown(f"""<div class="fkpi">
                    <div class="fkpi-val" style="color:{clr}">{val}</div>
                    <div class="fkpi-lbl">{lbl}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)
        cf,ct = st.columns([1.3,1])
        with cf:
            lbls = ["Nieuwe Vacatures","1e Gesprek","2e Gesprek","Aanbod","Geplaatst"]
            vals = [stats["nieuwe_vacatures"],stats["eerste_gesprek"],stats["tweede_gesprek"],stats["aanbod"],stats["geplaatst"]]
            clrs = ["#00d4c8","#e92076","#f5a623","#a78bfa","#00e5a0"]
            fig3 = go.Figure(go.Funnel(y=lbls, x=vals, textinfo="value+percent initial",
                marker=dict(color=clrs, line=dict(width=0)),
                connector=dict(line=dict(color="rgba(255,255,255,0.05)",width=2)),
                textfont=dict(size=15,color="white",family="Inter")))
            fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font_color="rgba(255,255,255,0.5)",
                margin=dict(l=180,r=40,t=10,b=10), height=320,
                yaxis=dict(tickfont=dict(size=13,family="Inter")))
            st.plotly_chart(fig3, use_container_width=True)
        with ct:
            arrow = "↑" if delta>=0 else "↓"
            clr2  = "#00e5a0" if delta>=0 else "#e92076"
            st.markdown(f"""
            <div class="card" style="margin-bottom:1rem"><div class="card-top"></div>
                <div class="card-label">vs vorige maand</div>
                <div style="font-family:Syne,sans-serif;font-size:2rem;font-weight:800;color:{clr2};margin-top:0.3rem">
                    {arrow} €{abs(delta):,.0f}
                </div>
                <div class="card-sub">{dpc:+.1f}%</div>
            </div>
            <div class="card"><div class="card-top"></div>
                <div class="card-label">Maandomzet {CURRENT_MONTH}</div>
                <div class="card-val pink" style="font-size:2.2rem">€{tot:,.0f}</div>
                <div class="card-sub">{pct:.0f}% van €{COMPANY_TARGET:,.0f}</div>
                <div class="prog"><div class="prog-fill" style="width:{pct:.1f}%"></div></div>
            </div>""", unsafe_allow_html=True)

    # ── SCREEN 2 — TOP VACATURES ──
    elif scr == 2:
        if not vacs:
            st.info("Geen vacatures.")
        else:
            idx = st.session_state["vac_idx"] % len(vacs)
            v   = vacs[idx]
            title   = v.get("jobTitle","—")
            company = (v.get("toCompany") or {}).get("name","—")
            ow      = v.get("owner") or {}
            cons    = f"{ow.get('firstName','')} {ow.get('lastName','')}".strip() or "—"
            ag      = (v.get("agency") or {}).get("name","—")
            matches = v.get("matchCount") or 0
            created = pd.to_datetime(v.get("creationDate")) if v.get("creationDate") else None
            days    = (nl_now()-created.replace(tzinfo=None)).days if created else 0
            loc     = v.get("workLocation") or v.get("rawWorkLocation") or ""
            func    = (v.get("toFunctionLevel1") or {}).get("label","")
            vno     = v.get("vacancyNo","")
            status  = v.get("statusDisplay","")

            pills = ""
            if func and func != "Dossier": pills += f'<span class="pill">{func}</span>'
            if loc: pills += f'<span class="pill">📍 {loc}</span>'
            if matches: pills += f'<span class="pill hot">🎯 {matches} kandidaten</span>'
            pills += f'<span class="pill">⏱ {days} dagen open</span>'
            dots = "".join([f'<div class="dot {"on" if i==idx else ""}"></div>' for i in range(len(vacs))])

            cm,cl = st.columns([1.6,1])
            with cm:
                st.markdown(f"""
                <div class="vac-big">
                    <div class="vac-meta-tag">Vacature {vno} · {status}</div>
                    <div class="vac-title">{title}</div>
                    <div class="vac-company">🏢 {company}</div>
                    <div class="pills">{pills}</div>
                    <div class="vac-recruiter">👤 {cons} · {ag}</div>
                </div>
                <div class="dots">{dots}</div>
                """, unsafe_allow_html=True)
                b1,b2,_ = st.columns([1,1,4])
                with b1:
                    if st.button("← Vorige", key="vp"):
                        st.session_state["vac_idx"] = (idx-1) % len(vacs); st.rerun(scope="fragment")
                with b2:
                    if st.button("Volgende →", key="vn"):
                        st.session_state["vac_idx"] = (idx+1) % len(vacs); st.rerun(scope="fragment")
            with cl:
                st.markdown('<div style="font-size:0.55rem;color:rgba(255,255,255,0.2);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.8rem;padding-left:0.5rem">Recente vacatures</div>', unsafe_allow_html=True)
                for i,vv in enumerate(vacs):
                    t2 = vv.get("jobTitle","—")
                    c2 = (vv.get("toCompany") or {}).get("name","—")
                    cls2 = "vac-sm cur" if i==idx else "vac-sm"
                    st.markdown(f'<div class="{cls2}"><div class="vac-sm-t">{t2}</div><div class="vac-sm-c">🏢 {c2}</div></div>', unsafe_allow_html=True)

render_screen()

# ── Live clock JS — injected at bottom after DOM ready ────────────────────────
st.markdown("""
<script>
(function(){
    var NL_D=['Zondag','Maandag','Dinsdag','Woensdag','Donderdag','Vrijdag','Zaterdag'];
    var NL_M=['januari','februari','maart','april','mei','juni','juli','augustus','september','oktober','november','december'];
    function getNL(){ return new Date(new Date().toLocaleString("en-US",{timeZone:"Europe/Amsterdam"})); }
    function tick(){
        var n=getNL();
        var c=('0'+n.getHours()).slice(-2)+':'+('0'+n.getMinutes()).slice(-2)+':'+('0'+n.getSeconds()).slice(-2);
        var d=NL_D[n.getDay()]+' '+n.getDate()+' '+NL_M[n.getMonth()]+' '+n.getFullYear();
        var ce=document.getElementById('clk'); if(ce) ce.textContent=c;
        var de=document.getElementById('clkd'); if(de) de.textContent=d.toUpperCase();
    }
    tick();
    setInterval(tick,1000);
})();
</script>
""", unsafe_allow_html=True)

st.markdown("""
<div style="text-align:center;padding:1rem 0 0.3rem;color:rgba(255,255,255,0.06);font-size:0.55rem;letter-spacing:3px;text-transform:uppercase">
    Data gecached · vernieuwd elke 2 uur
</div>
</div>
""", unsafe_allow_html=True)