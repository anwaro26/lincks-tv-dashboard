import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import time as _time
from datetime import datetime, timedelta
import requests
import base64
import streamlit.components.v1 as components
import numpy as np
import calendar

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
.hdr{display:flex;align-items:center;margin-bottom:1.5rem;}
.hdr-left{display:flex;align-items:center;gap:1.2rem;}
.hdr-badge{background:#e92076;color:white;font-size:0.55rem;font-weight:700;
  letter-spacing:3px;padding:0.25rem 0.7rem;border-radius:3px;text-transform:uppercase;}
.divider{height:1px;background:rgba(255,255,255,0.06);margin-bottom:1.2rem;}
.card{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.07);
  border-radius:14px;padding:1.5rem;position:relative;overflow:hidden;}
.card-top{position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,#e92076,transparent);}
.card-label{font-size:0.58rem;font-weight:600;color:rgba(255,255,255,0.25);
  text-transform:uppercase;letter-spacing:3px;margin-bottom:0.7rem;}
.card-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.pink{color:#e92076;}.teal{color:#00d4c8;}.green{color:#00e5a0;}.orange{color:#f5a623;}
.card-sub{font-size:0.7rem;color:rgba(255,255,255,0.25);margin-top:0.5rem;}
.prog{height:2px;background:rgba(255,255,255,0.06);border-radius:2px;margin-top:1rem;overflow:hidden;}
.prog-fill{height:100%;background:linear-gradient(90deg,#e92076,#ff6ab0);border-radius:2px;}
.fkpi{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);
  border-radius:12px;padding:1.5rem 1rem;text-align:center;}
.fkpi-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.fkpi-lbl{font-size:0.58rem;color:rgba(255,255,255,0.3);letter-spacing:2px;
  text-transform:uppercase;margin-top:0.5rem;}

/* Vacancy carousel */
.vac-stage{position:relative;width:100%;overflow:hidden;border-radius:18px;}
.vac-big{
  background:linear-gradient(145deg,#150330 0%,#220844 60%,#1a0535 100%);
  border:1px solid rgba(233,32,118,0.25);border-radius:18px;
  padding:2.2rem 2.5rem;position:relative;overflow:hidden;min-height:340px;
  animation:slidein 0.4s cubic-bezier(0.4,0,0.2,1);
}
@keyframes slidein{from{opacity:0;transform:translateX(40px);}to{opacity:1;transform:translateX(0);}}
.vac-big::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,#e92076 0%,#63ccca 50%,transparent 100%);}
.vac-big::after{content:'';position:absolute;top:-80px;right:-80px;width:300px;height:300px;
  background:radial-gradient(circle,rgba(233,32,118,0.06) 0%,transparent 70%);pointer-events:none;}
.vac-num{font-size:0.58rem;color:rgba(255,255,255,0.2);letter-spacing:3px;
  text-transform:uppercase;margin-bottom:0.7rem;}
.vac-title{font-family:'Syne',sans-serif;font-size:2.4rem;font-weight:800;
  color:white;line-height:1.1;margin-bottom:0.6rem;}
.vac-company{font-size:1rem;color:#00d4c8;font-weight:600;margin-bottom:0.5rem;}
.vac-desc{font-size:0.82rem;color:rgba(255,255,255,0.4);line-height:1.6;
  margin-bottom:1.2rem;max-height:60px;overflow:hidden;}
.pills{display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem;}
.pill{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);
  border-radius:20px;padding:0.3rem 0.85rem;font-size:0.72rem;color:rgba(255,255,255,0.55);}
.pill.acc{background:rgba(0,212,200,0.1);border-color:rgba(0,212,200,0.25);color:#00d4c8;}
.vac-recruiter{font-size:0.7rem;color:rgba(255,255,255,0.3);margin-top:0.8rem;}
.dots{display:flex;gap:0.35rem;justify-content:center;margin-top:1rem;}
.dot{width:5px;height:5px;border-radius:50%;background:rgba(255,255,255,0.1);}
.dot.on{background:#e92076;width:18px;border-radius:3px;}

/* Vacancy list */
.vac-sm{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05);
  border-left:2px solid rgba(233,32,118,0.3);border-radius:8px;
  padding:0.65rem 0.9rem;margin-bottom:0.4rem;}
.vac-sm.cur{border-left-color:#00d4c8;background:rgba(0,212,200,0.03);}
.vac-sm-t{font-size:0.8rem;font-weight:600;color:white;margin-bottom:0.1rem;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.vac-sm-c{font-size:0.65rem;color:rgba(255,255,255,0.28);}

/* Nav tabs */
.nav-tabs{display:flex;gap:0.4rem;margin-bottom:1.4rem;align-items:center;}
.stButton>button{
  background:transparent!important;color:rgba(255,255,255,0.28)!important;
  border:1px solid rgba(255,255,255,0.07)!important;border-radius:6px!important;
  font-size:0.62rem!important;letter-spacing:2px!important;text-transform:uppercase!important;
  font-weight:500!important;padding:0.3rem 0.75rem!important;transition:all 0.15s!important;
}
.stButton>button:hover{
  background:rgba(233,32,118,0.08)!important;
  border-color:rgba(233,32,118,0.2)!important;
  color:rgba(255,255,255,0.6)!important;
}
.stSelectbox>div>div{background:rgba(255,255,255,0.04)!important;
  border:1px solid rgba(255,255,255,0.08)!important;color:white!important;
  border-radius:8px!important;font-size:0.8rem!important;}
</style>
<meta http-equiv="refresh" content="7200">
""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID     = "4db4f54a9c90230221da81f085ef3bd5.apps.carerix.io"
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
TOKEN_URL     = "https://id-s3.carerix.io/auth/realms/lincks/protocol/openid-connect/token"
API_URL       = "https://api.carerix.io/graphql/v1/graphql"
DATA_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

ACTIVE_STATUSES = ["Actief1", "Actief2", "Actief - Startfee"]

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
    r=requests.post(TOKEN_URL,data={"grant_type":"client_credentials",
      "client_id":CLIENT_ID,"client_secret":CLIENT_SECRET})
    return r.json()["access_token"]

def run_query(q):
    t=get_token()
    r=requests.post(API_URL,json={"query":q},
      headers={"Authorization":f"Bearer {t}"},timeout=60)
    return r.json()

def get_last_sync():
    p=os.path.join(DATA_DIR,"sync_meta.parquet")
    if os.path.exists(p):
        try: return str(pd.read_parquet(p)["last_incremental_sync"].iloc[0])[:10]
        except: pass
    return "2026-04-02"

def days_remaining():
    n=nl_now()
    last_day=calendar.monthrange(n.year,n.month)[1]
    return last_day-n.day+1

def total_days():
    n=nl_now()
    return calendar.monthrange(n.year,n.month)[1]

def compute_forecast(inv_raw, current_tot):
    try:
        today_day=nl_now().day
        cur_month_num=nl_now().month
        df=inv_raw.copy()
        df["_date"]=pd.to_datetime(df.get("date",df.get("value_date","")),errors="coerce")
        df=df[df["_date"].notna()].copy()
        df["_month"]=df["_date"].dt.strftime("%Y-%m")
        df["_day"]=df["_date"].dt.day
        df["_total"]=pd.to_numeric(df.get("total",0),errors="coerce").fillna(0)
        df["_status"]=df.get("status","").astype(str)
        df=df[(df["_status"]=="Verzonden")&(df["_month"]!=CURRENT_MONTH)]
        if df.empty: return None
        same,other=[],[]
        for m in df["_month"].unique():
            mdf=df[df["_month"]==m]
            mt=mdf["_total"].sum()
            if mt<=0: continue
            frac=mdf[mdf["_day"]<=today_day]["_total"].sum()/mt
            if frac<=0 or frac>1: continue
            if int(m.split("-")[1])==cur_month_num: same.append(frac)
            else: other.append(frac)
        if same and other: wf=0.6*np.mean(same)+0.4*np.mean(other)
        elif same: wf=np.mean(same)
        elif other: wf=np.mean(other)
        else: return None
        raw = round(current_tot/wf) if wf>0.01 else None
        if raw is None: return None
        # Cap: forecast can't be less than current, or more than 3x current
        raw = max(raw, current_tot)
        raw = min(raw, current_tot * 3)
        # On last few days, forecast = current (no room to grow much)
        days_left = days_remaining()
        tot_d = total_days()
        if days_left <= 2:
            return round(current_tot * 1.02)
        return raw
    except Exception as e:
        print(f"[FORECAST] {e}"); return None

def compute_daily_revenue(inv_raw):
    """Returns list of (day, cumulative_revenue) for current month."""
    try:
        df=inv_raw.copy()
        df["_date"]=pd.to_datetime(df.get("date",df.get("value_date","")),errors="coerce")
        df["_month"]=df["_date"].dt.strftime("%Y-%m")
        df["_day"]=df["_date"].dt.day
        df["_total"]=pd.to_numeric(df.get("total",0),errors="coerce").fillna(0)
        df["_status"]=df.get("status","").astype(str)
        cur=df[(df["_status"]=="Verzonden")&(df["_month"]==CURRENT_MONTH)]
        if cur.empty: return [],[]
        daily=cur.groupby("_day")["_total"].sum()
        days=list(range(1,nl_now().day+1))
        cumrev=[daily.get(d,0) for d in days]
        cumrev=[sum(cumrev[:i+1]) for i in range(len(cumrev))]
        return days,cumrev
    except: return [],[]

@st.cache_data(ttl=7200, show_spinner=False)
def load_invoice_df():
    users_df=pd.read_parquet(os.path.join(DATA_DIR,"users.parquet"))
    users=dict(zip(users_df["user_id"],users_df["full_name"]))
    pl=pd.read_parquet(os.path.join(DATA_DIR,"placements.parquet"))
    inv_raw=pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet"))

    # Incremental
    last_sync=get_last_sync()
    today=nl_now().strftime("%Y-%m-%d")
    if last_sync<today and CLIENT_SECRET:
        try:
            probe=run_query("{ crInvoicePage(pageable:{page:0,size:1}){ totalElements } }")
            total=(probe.get("data",{}).get("crInvoicePage") or {}).get("totalElements",0)
            if total:
                lp=(total-1)//100
                for pn in range(max(0,lp-1),lp+1):
                    q=f"""{{ crInvoicePage(pageable:{{page:{pn},size:100}}){{
                        items{{ _id invoiceID valueDate date total statusDisplay
                               companyName toCompany{{name}} toJob{{_id}}
                               agency{{name}} toInvoice{{_id valueDate date}} }}
                    }} }}"""
                    data=run_query(q)
                    items=(data.get("data",{}).get("crInvoicePage") or {}).get("items",[])
                    rows=[]
                    for i in items:
                        vd=str(i.get("valueDate") or "")[:10]
                        d=str(i.get("date") or "")[:10]
                        ds=max(vd,d) if vd and d else (vd or d)
                        if ds and ds>last_sync:
                            og=i.get("toInvoice") or {}
                            co=(i.get("toCompany") or {}).get("name") or i.get("companyName") or "Unknown"
                            ag=(i.get("agency") or {}).get("name","Unknown")
                            jb=i.get("toJob") or {}
                            rows.append({"invoice_id":i.get("invoiceID"),
                                "internal_id":str(i["_id"]),"value_date":i.get("valueDate"),
                                "date":i.get("date"),
                                "original_date":og.get("valueDate") or og.get("date"),
                                "total":float(i.get("total") or 0),
                                "status":i.get("statusDisplay"),"company":co,"agency":ag,
                                "job_id":str(jb.get("_id","")) if jb else ""})
                    if rows:
                        new_df=pd.DataFrame(rows)
                        inv_raw=pd.concat([inv_raw,new_df],ignore_index=True).drop_duplicates(
                            subset=["internal_id"],keep="last")
                    _time.sleep(0.3)
        except Exception as e: print(f"[FETCH] {e}")

    # Daily revenue for chart
    daily_days, daily_rev = compute_daily_revenue(inv_raw)

    # Forecast
    cur_tot_approx=sum(daily_rev) if daily_rev else 0
    forecast=compute_forecast(inv_raw, cur_tot_approx)

    # Filter to current+prev month
    inv_raw["_vd"]=pd.to_datetime(inv_raw.get("value_date",""),errors="coerce")
    inv_raw["_d"]=pd.to_datetime(inv_raw.get("date",""),errors="coerce")
    inv_raw["_best"]=inv_raw[["_vd","_d"]].max(axis=1)
    inv_raw["_month"]=inv_raw["_best"].dt.strftime("%Y-%m")
    inv_filtered=inv_raw[inv_raw["_month"].isin([CURRENT_MONTH,PREV_MONTH])].copy()

    job_map={str(p["placement_id"]):p for _,p in pl.iterrows()}
    rows=[]
    for _,i in inv_filtered.iterrows():
        amt=float(i.get("total") or 0)
        if amt==0: continue
        if str(i.get("status") or "")!="Verzonden": continue
        vd=str(i.get("value_date") or "")[:10]
        d=str(i.get("date") or "")[:10]
        og=str(i.get("original_date") or "")[:10]
        if amt<0 and og and og not in ("None","nan",""): ds=og
        else: ds=max(vd,d) if vd and d else (vd or d)
        ds=ds.replace("None","").replace("nan","").strip()
        if not ds: continue
        try: date=pd.to_datetime(ds)
        except: continue
        month=date.strftime("%Y-%m")
        if month not in [CURRENT_MONTH,PREV_MONTH]: continue
        co=str(i.get("company") or "Unknown")
        ag=str(i.get("agency") or "Unknown")
        jid=str(i.get("job_id") or "")
        adj=amt*MARGINS.get(co,1.0) if ag=="Lincks Personeel B.V." else amt
        cons=[]
        if jid and jid in job_map:
            p=job_map[jid]
            fu=list(set([str(p[k]) for k in ["fase_6525","fase_6526","fase_6527",
               "fase_6528","fase_6538"] if k in p.index and str(p[k]) not in ("","nan","None")]))
            cons=[users.get(u,f"User {u}") for u in fu]
        if not cons: cons=["Mireille Prooi"]
        rp=adj/len(cons)
        for c in cons:
            rows.append({"consultant":c,"amount":adj,"revenue":rp,
                "date":date,"month":month})
    return pd.DataFrame(rows), forecast, daily_days, daily_rev

@st.cache_data(ttl=7200, show_spinner=False)
def load_pipeline():
    """Load pipeline from matches parquet — fast, no API calls needed."""
    s={"eerste_gesprek":0,"tweede_gesprek":0,"aanbod":0,"geplaatst":0,
       "voorgesteld":0,"avg_ttf":None,"avg_tth":None}
    try:
        # Load matches parquet
        matches = pd.read_parquet(os.path.join(DATA_DIR,"matches.parquet"))
        matches["creation_date"] = pd.to_datetime(matches.get("creation_date",""), errors="coerce")
        matches["month"] = matches["creation_date"].dt.strftime("%Y-%m")
        cur_m = matches[matches["month"] == CURRENT_MONTH].copy()

        if not cur_m.empty:
            for _, row in cur_m.iterrows():
                sn = str(row.get("status_display") or row.get("status_name") or "")
                # Voorgesteld = any active stage (not final rejected)
                if not row.get("is_final", False):
                    s["voorgesteld"] += 1
                # 1e gesprek klant = stages 4.x
                if any(f"4.{x}" in sn for x in ["1","2","3","4","5"]):
                    s["eerste_gesprek"] += 1
                # 2e gesprek = 4.2+
                if any(f"4.{x}" in sn for x in ["2","3","4","5"]) or "2e gesprek" in sn.lower():
                    s["tweede_gesprek"] += 1
                # Aanbod = 5.x
                if sn.startswith("5.") or "aanbod" in sn.lower():
                    s["aanbod"] += 1
                # Geplaatst
                if row.get("is_successfully_filled") or row.get("has_job"):
                    s["geplaatst"] += 1

        # Placements for geplaatst count + TTF/TTH
        pl = pd.read_parquet(os.path.join(DATA_DIR,"placements.parquet"))
        pl["creation_date"] = pd.to_datetime(pl.get("creation_date",""), errors="coerce")
        pl["vacancy_created"] = pd.to_datetime(pl.get("vacancy_created",""), errors="coerce")
        pl["match_created"] = pd.to_datetime(pl.get("match_created",""), errors="coerce")
        pl["month"] = pl["creation_date"].dt.strftime("%Y-%m")
        cur_pl = pl[pl["month"] == CURRENT_MONTH].copy()

        # Use placements count as geplaatst (more reliable)
        if not cur_pl.empty:
            s["geplaatst"] = len(cur_pl)
            ttf = (cur_pl["creation_date"]-cur_pl["vacancy_created"]).dt.days.dropna()
            tth = (cur_pl["creation_date"]-cur_pl["match_created"]).dt.days.dropna()
            ttf = ttf[(ttf>0)&(ttf<500)]
            tth = tth[(tth>0)&(tth<500)]
            if not ttf.empty: s["avg_ttf"] = round(ttf.mean())
            if not tth.empty: s["avg_tth"] = round(tth.mean())

        # If matches parquet is empty for current month, try API
        if s["voorgesteld"] == 0 and CLIENT_SECRET:
            ms = f"{CURRENT_MONTH}-01"
            q2 = f"""{{ crJobPage(qualifier:"creationDate > (NSCalendarDate) '{ms} 00:00:00'",
                pageable:{{page:0,size:1}}){{ totalElements }} }}"""
            s["geplaatst"] = max(s["geplaatst"],
                (run_query(q2).get("data",{}).get("crJobPage") or {}).get("totalElements",0))

    except Exception as e: print(f"[PIPELINE] {e}")
    return s

@st.cache_data(ttl=7200, show_spinner=False)
def load_vacancies():
    """Paginate backwards from last page to find recent active vacancies fast."""
    # First get total
    probe = run_query("{ crVacancyPage(pageable:{page:0,size:1}){ totalElements } }")
    total = (probe.get("data",{}).get("crVacancyPage") or {}).get("totalElements", 0)
    if not total: return [], 0

    page_size = 100
    last_page = (total - 1) // page_size
    result = []
    # Search backwards from most recent pages
    for pn in range(last_page, max(-1, last_page - 30), -1):
        q=f"""{{ crVacancyPage(pageable:{{page:{pn},size:{page_size}}}){{
            totalElements items{{
                _id jobTitle vacancyNo creationDate statusDisplay
                toCompany{{name}} owner{{firstName lastName}} agency{{name}}
                workLocation rawWorkLocation toFunctionLevel1{{label}}
                jobDescription
            }}
        }} }}"""
        data=run_query(q)
        if not data or not data.get("data"): break
        items=(data.get("data",{}).get("crVacancyPage") or {}).get("items",[])
        if not items: break
        for v in items:
            if v.get("statusDisplay") in ACTIVE_STATUSES                and v.get("jobTitle") and v.get("jobTitle") not in ("--","None"):
                result.append(v)
        if len(result) >= 10: break
        _time.sleep(0.2)

    # Also check first few pages for active ones
    if len(result) < 10:
        for pn in range(min(5, last_page)):
            q=f"""{{ crVacancyPage(pageable:{{page:{pn},size:{page_size}}}){{
                items{{
                    _id jobTitle vacancyNo creationDate statusDisplay
                    toCompany{{name}} owner{{firstName lastName}} agency{{name}}
                    workLocation rawWorkLocation toFunctionLevel1{{label}}
                    jobDescription
                }}
            }} }}"""
            data=run_query(q)
            if not data or not data.get("data"): break
            items=(data.get("data",{}).get("crVacancyPage") or {}).get("items",[])
            for v in items:
                if v.get("statusDisplay") in ACTIVE_STATUSES                    and v.get("jobTitle") and v.get("jobTitle") not in ("--","None"):
                    if not any(x["_id"]==v["_id"] for x in result):
                        result.append(v)
            _time.sleep(0.2)

    total_count = len(result)
    result.sort(key=lambda x:x.get("creationDate",""), reverse=True)
    return result[:10], total_count

# ── Load all ──────────────────────────────────────────────────────────────────
with st.spinner(""):
    try:
        df,forecast,daily_days,daily_rev=load_invoice_df()
        pipeline=load_pipeline()
        vacs,open_vac_count=load_vacancies()
        data_ok=True
    except Exception as e:
        st.error(f"Error: {e}"); data_ok=False

if not data_ok or df.empty:
    st.warning("Geen data."); st.stop()

# Pre-compute
df_cur=df[df["month"]==CURRENT_MONTH]
df_prev=df[df["month"]==PREV_MONTH]
tot=df_cur.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
prev_tot=df_prev.drop_duplicates(subset=["consultant","amount","date"])["amount"].sum()
pct=min(tot/COMPANY_TARGET*100,100)
rem=max(COMPANY_TARGET-tot,0)
delta=tot-prev_tot
dpc=(delta/prev_tot*100) if prev_tot>0 else 0
excl=df_cur[~df_cur["consultant"].isin(["Mireille Prooi","Onbekend"])]
days_rem=days_remaining()
tot_days=total_days()

# Recompute forecast with actual tot
forecast=compute_forecast(
    pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet")), tot) if forecast is None else forecast

# ── State ─────────────────────────────────────────────────────────────────────
if "screen"  not in st.session_state: st.session_state["screen"]=0
if "vac_idx" not in st.session_state: st.session_state["vac_idx"]=0
if "locked"  not in st.session_state: st.session_state["locked"]=False
if "last_sw" not in st.session_state: st.session_state["last_sw"]=_time.time()
if "vac_ts"  not in st.session_state: st.session_state["vac_ts"]=_time.time()

now_t=_time.time()
if not st.session_state["locked"] and now_t-st.session_state["last_sw"]>60:
    st.session_state["screen"]=1-st.session_state["screen"]
    st.session_state["last_sw"]=now_t
if st.session_state["screen"]==2 and now_t-st.session_state["vac_ts"]>6:
    st.session_state["vac_idx"]=(st.session_state["vac_idx"]+1)
    st.session_state["vac_ts"]=now_t

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path=os.path.join(os.path.dirname(os.path.abspath(__file__)),"lincks_logo_fc-wit_def.png")
if os.path.exists(logo_path):
    with open(logo_path,"rb") as f: lb64=base64.b64encode(f.read()).decode()
    logo_html=f'<img src="data:image/png;base64,{lb64}" style="height:40px;" />'
else:
    logo_html='<span style="font-family:Syne,sans-serif;font-size:1.4rem;font-weight:800;">LINCKS</span>'

scr=st.session_state["screen"]

st.markdown('<div class="wrap">', unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
hc1,hc2=st.columns([3,1])
with hc1:
    st.markdown(f"""<div class="hdr">
        <div class="hdr-left">
            {logo_html}
            <span class="hdr-badge">Live · {CURRENT_MONTH}</span>
        </div>
    </div>""", unsafe_allow_html=True)
with hc2:
    components.html("""<!DOCTYPE html><html><head>
    <link href="https://fonts.googleapis.com/css2?family=Syne:wght@800&family=Inter:wght@500&display=swap" rel="stylesheet">
    <style>
    *{margin:0;padding:0;box-sizing:border-box;}body{background:transparent;overflow:hidden;}
    #w{text-align:right;padding:0.2rem 0;}
    #c{font-family:'Syne','Arial Black',sans-serif;font-size:2.6rem;font-weight:800;
       color:#e92076;line-height:1;letter-spacing:-1px;text-shadow:0 0 30px rgba(233,32,118,0.4);}
    #d{font-family:'Inter',sans-serif;font-size:0.58rem;color:rgba(255,255,255,0.22);
       letter-spacing:3px;text-transform:uppercase;margin-top:3px;}
    </style></head><body><div id="w"><div id="c">--:--:--</div><div id="d">--</div></div>
    <script>
    var D=['Zondag','Maandag','Dinsdag','Woensdag','Donderdag','Vrijdag','Zaterdag'];
    var M=['januari','februari','maart','april','mei','juni','juli','augustus',
           'september','oktober','november','december'];
    function getNL(){return new Date(new Date().toLocaleString("en-US",{timeZone:"Europe/Amsterdam"}));}
    function tick(){
        var n=getNL();
        var c=('0'+n.getHours()).slice(-2)+':'+('0'+n.getMinutes()).slice(-2)+':'+('0'+n.getSeconds()).slice(-2);
        var d=D[n.getDay()]+' '+n.getDate()+' '+M[n.getMonth()]+' '+n.getFullYear();
        document.getElementById('c').textContent=c;
        document.getElementById('d').textContent=d.toUpperCase();
    }
    tick();setInterval(tick,1000);
    </script></body></html>""", height=80, scrolling=False)

st.markdown('<div class="divider" style="margin-top:0.5rem"></div>', unsafe_allow_html=True)

# ── Nav ───────────────────────────────────────────────────────────────────────
@st.fragment
def render_nav():
    n1,n2,n3=st.columns([1,1,2])
    with n1:
        if st.button("Omzet", key="b0"):
            st.session_state.update({"screen":0,"locked":False,"last_sw":_time.time()})
            st.rerun(scope="app")
    with n2:
        if st.button("Pipeline", key="b1"):
            st.session_state.update({"screen":1,"locked":False,"last_sw":_time.time()})
            st.rerun(scope="app")
    with n3:
        if st.button("★  Vacatures  (vergrendelt scherm)", key="b2"):
            st.session_state.update({"screen":2,"locked":True,"vac_idx":0,"vac_ts":_time.time()})
            st.rerun(scope="app")
    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

render_nav()

# ════════════════════════════════════════════════════════════════
# SCREENS
# ════════════════════════════════════════════════════════════════
@st.fragment
def render_screen():
    scr=st.session_state["screen"]

    # ── OMZET ────────────────────────────────────────────────────────────────
    if scr==0:
        k1,k2,k3,k4=st.columns(4)
        with k1:
            dc="#00e5a0" if days_rem<=3 else "#f5a623" if days_rem<=7 else "#00d4c8"
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Dagen resterend</div>
                <div class="card-val" style="color:{dc}">{days_rem}</div>
                <div class="card-sub">van {tot_days} dagen in {CURRENT_MONTH}</div>
            </div>""", unsafe_allow_html=True)
        with k2:
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Maandomzet</div>
                <div class="card-val pink">€{tot:,.0f}</div>
                <div class="card-sub">{pct:.0f}% van €{COMPANY_TARGET:,.0f} target</div>
                <div class="prog"><div class="prog-fill" style="width:{pct:.1f}%"></div></div>
            </div>""", unsafe_allow_html=True)
        with k3:
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Open Vacatures</div>
                <div class="card-val teal">{open_vac_count}</div>
                <div class="card-sub">actief uitstaand</div>
            </div>""", unsafe_allow_html=True)
        with k4:
            if forecast and forecast>0:
                fp=min(forecast/COMPANY_TARGET*100,150)
                fc="#00e5a0" if fp>=100 else "#f5a623" if fp>=80 else "#e92076"
                ft=f"€{forecast:,.0f}"
                fs=f"{fp:.0f}% van target · historisch dagpatroon"
            else:
                fc="rgba(255,255,255,0.25)"; ft="—"; fs="onvoldoende data"
            st.markdown(f"""<div class="card"><div class="card-top"></div>
                <div class="card-label">Omzetverwachting</div>
                <div class="card-val" style="color:{fc};font-size:2.2rem">{ft}</div>
                <div class="card-sub">{fs}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

        cd,cb=st.columns([1,2.2])
        with cd:
            fig=go.Figure(go.Pie(values=[tot,rem],labels=["Behaald","Resterend"],
                hole=0.8,sort=False,textinfo="none",
                marker_colors=["#e92076","rgba(255,255,255,0.04)"],
                marker=dict(line=dict(width=0))))
            fig.add_annotation(text=f"<b>{pct:.0f}%</b>",x=0.5,y=0.58,
                font=dict(size=56,color="#e92076",family="Syne"),showarrow=False)
            fig.add_annotation(text=f"€{tot:,.0f}",x=0.5,y=0.40,
                font=dict(size=15,color="rgba(255,255,255,0.6)"),showarrow=False)
            fig.add_annotation(text=f"van €{COMPANY_TARGET:,.0f}",x=0.5,y=0.27,
                font=dict(size=10,color="rgba(255,255,255,0.22)"),showarrow=False)
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,margin=dict(l=10,r=10,t=10,b=10),height=300)
            st.plotly_chart(fig,use_container_width=True)

        with cb:
            # Revenue chart with forecast line
            if daily_days and daily_rev:
                all_days=list(range(1,tot_days+1))
                # Actual line up to today
                act_x=daily_days; act_y=daily_rev
                # Forecast dotted line from today to end of month
                if forecast and len(act_y)>0:
                    last_val=act_y[-1]
                    rem_days=tot_days-daily_days[-1]
                    if rem_days>0:
                        step=(forecast-last_val)/rem_days
                        fc_x=list(range(daily_days[-1],tot_days+1))
                        fc_y=[last_val+step*i for i in range(len(fc_x))]
                    else:
                        fc_x,fc_y=[],[]
                else:
                    fc_x,fc_y=[],[]

                fig_rev=go.Figure()
                fig_rev.add_trace(go.Scatter(
                    x=act_x,y=act_y,mode="lines",name="Werkelijk",
                    line=dict(color="#e92076",width=3),
                    fill="tozeroy",fillcolor="rgba(233,32,118,0.08)"))
                if fc_x:
                    fig_rev.add_trace(go.Scatter(
                        x=fc_x,y=fc_y,mode="lines",name="Verwachting",
                        line=dict(color="#f5a623",width=2,dash="dot")))
                    fig_rev.add_hline(y=COMPANY_TARGET,
                        line=dict(color="rgba(255,255,255,0.15)",width=1,dash="dash"),
                        annotation_text="Target",
                        annotation_font=dict(color="rgba(255,255,255,0.3)",size=10))
                fig_rev.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",
                    font_color="rgba(255,255,255,0.4)",
                    xaxis=dict(gridcolor="rgba(255,255,255,0.04)",
                        tickvals=list(range(1,tot_days+1,5)),title="Dag",
                        tickfont=dict(size=10)),
                    yaxis=dict(gridcolor="rgba(255,255,255,0.04)",
                        tickprefix="€",tickfont=dict(size=10)),
                    legend=dict(orientation="h",yanchor="bottom",y=1.01,
                        xanchor="right",x=1,font=dict(color="rgba(255,255,255,0.4)",size=10)),
                    margin=dict(l=0,r=10,t=30,b=0),height=240)
                st.plotly_chart(fig_rev,use_container_width=True)
            else:
                # Fallback: recruiter bar chart
                def get_t(n):
                    if n in TARGETS: return TARGETS[n]
                    last=n.split()[-1] if n else ""
                    for k,v in TARGETS.items():
                        if k.split()[-1]==last: return v
                    return DEFAULT_TARGET
                rc=excl.groupby("consultant")["revenue"].sum().reset_index()
                rc["t"]=rc["consultant"].apply(get_t)
                rc["p"]=(rc["revenue"]/rc["t"]*100).round(1)
                rc["r"]=(rc["t"]-rc["revenue"]).clip(lower=0)
                rc=rc.sort_values("revenue",ascending=True)
                rc["c"]=rc["p"].apply(lambda x:"#00e5a0" if x>=100 else("#f5a623" if x>=80 else"#e92076"))
                fig2=go.Figure()
                fig2.add_trace(go.Bar(x=rc["revenue"],y=rc["consultant"],orientation="h",
                    marker_color=rc["c"],marker_line_width=0,
                    text=rc.apply(lambda r:f"  €{r['revenue']:,.0f}  ·  {r['p']:.0f}%",axis=1),
                    textposition="inside",insidetextanchor="start",
                    textfont=dict(color="white",size=11,family="Inter")))
                fig2.add_trace(go.Bar(x=rc["r"],y=rc["consultant"],orientation="h",
                    marker_color="rgba(255,255,255,0.03)",
                    marker_line_color="rgba(255,255,255,0.06)",marker_line_width=1))
                fig2.update_layout(barmode="stack",plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",font_color="rgba(255,255,255,0.4)",
                    xaxis=dict(gridcolor="rgba(255,255,255,0.04)",tickprefix="€",
                        tickfont=dict(size=10),zeroline=False),
                    yaxis=dict(tickfont=dict(size=12,family="Inter"),gridcolor="rgba(0,0,0,0)"),
                    margin=dict(l=0,r=10,t=10,b=0),height=300,bargap=0.2,showlegend=False)
                st.plotly_chart(fig2,use_container_width=True)

        # Recruiter bar always shown below
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        def get_t(n):
            if n in TARGETS: return TARGETS[n]
            last=n.split()[-1] if n else ""
            for k,v in TARGETS.items():
                if k.split()[-1]==last: return v
            return DEFAULT_TARGET
        rc=excl.groupby("consultant")["revenue"].sum().reset_index()
        rc["t"]=rc["consultant"].apply(get_t)
        rc["p"]=(rc["revenue"]/rc["t"]*100).round(1)
        rc["r"]=(rc["t"]-rc["revenue"]).clip(lower=0)
        rc=rc.sort_values("revenue",ascending=True)
        rc["c"]=rc["p"].apply(lambda x:"#00e5a0" if x>=100 else("#f5a623" if x>=80 else"#e92076"))
        fig3=go.Figure()
        fig3.add_trace(go.Bar(x=rc["revenue"],y=rc["consultant"],orientation="h",name="Behaald",
            marker_color=rc["c"],marker_line_width=0,
            text=rc.apply(lambda r:f"  €{r['revenue']:,.0f}  ·  {r['p']:.0f}%",axis=1),
            textposition="inside",insidetextanchor="start",
            textfont=dict(color="white",size=12,family="Inter")))
        fig3.add_trace(go.Bar(x=rc["r"],y=rc["consultant"],orientation="h",name="Resterend",
            marker_color="rgba(255,255,255,0.03)",
            marker_line_color="rgba(255,255,255,0.06)",marker_line_width=1))
        fig3.update_layout(barmode="stack",plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",font_color="rgba(255,255,255,0.4)",
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)",tickprefix="€",
                tickfont=dict(size=10),zeroline=False),
            yaxis=dict(tickfont=dict(size=13,family="Inter"),gridcolor="rgba(0,0,0,0)"),
            legend=dict(orientation="h",yanchor="bottom",y=1.01,xanchor="right",x=1,
                font=dict(color="rgba(255,255,255,0.3)",size=10)),
            margin=dict(l=0,r=10,t=30,b=0),height=280,bargap=0.2)
        st.plotly_chart(fig3,use_container_width=True)

    # ── PIPELINE ─────────────────────────────────────────────────────────────
    elif scr==1:
        p=pipeline
        ttf=f"{p['avg_ttf']} dgn" if p['avg_ttf'] else "—"
        tth=f"{p['avg_tth']} dgn" if p['avg_tth'] else "—"

        kpis=[
            ("Voorgesteld",     p["voorgesteld"],    "#00d4c8"),
            ("1e Gesprek Klant",p["eerste_gesprek"], "#e92076"),
            ("2e Gesprek Klant",p["tweede_gesprek"],  "#f5a623"),
            ("Heeft Aanbod",    p["aanbod"],          "#a78bfa"),
            ("Geplaatst ✓",     p["geplaatst"],       "#00e5a0"),
        ]
        cols=st.columns(5)
        for col,(lbl,val,clr) in zip(cols,kpis):
            with col:
                st.markdown(f"""<div class="fkpi">
                    <div class="fkpi-val" style="color:{clr}">{val}</div>
                    <div class="fkpi-lbl">{lbl}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        cf,ct=st.columns([1.4,1])
        with cf:
            lbls=["Voorgesteld","1e Gesprek","2e Gesprek","Aanbod","Geplaatst"]
            vals=[p["voorgesteld"],p["eerste_gesprek"],p["tweede_gesprek"],p["aanbod"],p["geplaatst"]]
            clrs=["#00d4c8","#e92076","#f5a623","#a78bfa","#00e5a0"]
            fig_f=go.Figure(go.Funnel(y=lbls,x=vals,textinfo="value+percent initial",
                marker=dict(color=clrs,line=dict(width=0)),
                connector=dict(line=dict(color="rgba(255,255,255,0.05)",width=2)),
                textfont=dict(size=14,color="white",family="Inter")))
            fig_f.update_layout(plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)",
                font_color="rgba(255,255,255,0.5)",
                margin=dict(l=160,r=40,t=10,b=10),height=300,
                yaxis=dict(tickfont=dict(size=12,family="Inter")))
            st.plotly_chart(fig_f,use_container_width=True)

        with ct:
            arrow="↑" if delta>=0 else "↓"
            clr2="#00e5a0" if delta>=0 else "#e92076"
            st.markdown(f"""
            <div class="card" style="margin-bottom:0.8rem"><div class="card-top"></div>
                <div class="card-label">vs vorige maand</div>
                <div style="font-family:Syne,sans-serif;font-size:1.8rem;font-weight:800;
                    color:{clr2};margin-top:0.2rem">{arrow} €{abs(delta):,.0f}</div>
                <div class="card-sub">{dpc:+.1f}%</div>
            </div>""", unsafe_allow_html=True)
            t1,t2=st.columns(2)
            with t1:
                st.markdown(f"""<div class="card"><div class="card-top"></div>
                    <div class="card-label">Gem. Time to Fill</div>
                    <div class="card-val teal" style="font-size:2rem">{ttf}</div>
                </div>""", unsafe_allow_html=True)
            with t2:
                st.markdown(f"""<div class="card"><div class="card-top"></div>
                    <div class="card-label">Gem. Time to Hire</div>
                    <div class="card-val pink" style="font-size:2rem">{tth}</div>
                </div>""", unsafe_allow_html=True)

    # ── VACATURES ────────────────────────────────────────────────────────────
    elif scr==2:
        if not vacs:
            st.info("Geen actieve vacatures.")
        else:
            idx=st.session_state["vac_idx"]%len(vacs)
            v=vacs[idx]
            title=v.get("jobTitle","—")
            company=(v.get("toCompany") or {}).get("name","—")
            ow=v.get("owner") or {}
            cons=f"{ow.get('firstName','')} {ow.get('lastName','')}".strip() or "—"
            ag=(v.get("agency") or {}).get("name","—")
            created=pd.to_datetime(v.get("creationDate")) if v.get("creationDate") else None
            days_open=(nl_now()-created.replace(tzinfo=None)).days if created else 0
            loc=v.get("workLocation") or v.get("rawWorkLocation") or ""
            func=(v.get("toFunctionLevel1") or {}).get("label","")
            vno=v.get("vacancyNo","")
            status=v.get("statusDisplay","")
            desc=v.get("jobDescription") or ""
            # Strip HTML tags from description
            import re
            desc=re.sub(r'<[^>]+>','',desc).strip()
            desc=desc[:180]+"…" if len(desc)>180 else desc

            pills=""
            if func and func!="Dossier": pills+=f'<span class="pill acc">{func}</span>'
            if loc: pills+=f'<span class="pill">📍 {loc}</span>'
            if status: pills+=f'<span class="pill">{status}</span>'
            days_c="#e92076" if days_open>60 else "#f5a623" if days_open>30 else "#00d4c8"
            pills+=f'<span class="pill" style="color:{days_c};border-color:rgba(255,255,255,0.12)">⏱ {days_open} dagen open</span>'

            dots="".join([f'<div class="dot {"on" if i==idx else ""}"></div>' for i in range(len(vacs))])

            # Counter
            st.markdown(f'<div style="font-size:0.58rem;color:rgba(255,255,255,0.2);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.8rem">{idx+1} / {len(vacs)} actieve vacatures · {open_vac_count} totaal</div>', unsafe_allow_html=True)

            cm,cl=st.columns([1.7,1])
            with cm:
                st.markdown(f"""
                <div class="vac-big">
                    <div class="vac-num">Vacature {vno}</div>
                    <div class="vac-title">{title}</div>
                    <div class="vac-company">🏢 {company}</div>
                    <div class="pills">{pills}</div>
                    {"<div class='vac-desc'>"+desc+"</div>" if desc else ""}
                    <div class="vac-recruiter">👤 Recruiter: {cons} · {ag}</div>
                </div>
                <div class="dots">{dots}</div>
                """, unsafe_allow_html=True)
                b1,b2,_=st.columns([1,1,4])
                with b1:
                    if st.button("← Vorige",key="vp"):
                        st.session_state["vac_idx"]=(idx-1)%len(vacs)
                        st.session_state["vac_ts"]=_time.time()
                        st.rerun(scope="fragment")
                with b2:
                    if st.button("Volgende →",key="vn"):
                        st.session_state["vac_idx"]=(idx+1)%len(vacs)
                        st.session_state["vac_ts"]=_time.time()
                        st.rerun(scope="fragment")

            with cl:
                st.markdown('<div style="font-size:0.52rem;color:rgba(255,255,255,0.18);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.6rem;padding-left:0.3rem">Recent actief</div>', unsafe_allow_html=True)
                for i,vv in enumerate(vacs):
                    t2=vv.get("jobTitle","—")
                    c2=(vv.get("toCompany") or {}).get("name","—")
                    ow2=vv.get("owner") or {}
                    r2=f"{ow2.get('firstName','')} {ow2.get('lastName','')}".strip()
                    cr2=pd.to_datetime(vv.get("creationDate")) if vv.get("creationDate") else None
                    do2=(nl_now()-cr2.replace(tzinfo=None)).days if cr2 else 0
                    cls2="vac-sm cur" if i==idx else "vac-sm"
                    dc2="#e92076" if do2>60 else "#f5a623" if do2>30 else "rgba(255,255,255,0.2)"
                    st.markdown(f"""<div class="{cls2}">
                        <div class="vac-sm-t">{t2}</div>
                        <div class="vac-sm-c">🏢 {c2} · 👤 {r2} · <span style="color:{dc2}">{do2}d</span></div>
                    </div>""", unsafe_allow_html=True)

render_screen()

st.markdown("""
<div style="text-align:center;padding:0.8rem 0 0.3rem;color:rgba(255,255,255,0.05);
    font-size:0.52rem;letter-spacing:3px;text-transform:uppercase">
    Data gecached · vernieuwd elke 2 uur
</div></div>
""", unsafe_allow_html=True)