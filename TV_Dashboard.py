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
from streamlit_autorefresh import st_autorefresh

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
# Try st.secrets first (Streamlit Cloud), fall back to env var (local dev)
try:
    CLIENT_SECRET = st.secrets.get("CLIENT_SECRET", os.environ.get("CLIENT_SECRET", ""))
except Exception:
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
    """
    Conservative linear extrapolation:
      base     = current_tot / (day_of_month / total_days)  — simple daily run-rate scale-up
      hard_cap = current_tot + days_remaining * daily_rate * 1.1  — never more than run-rate + 10%
    Returns current_tot unchanged in last 3 days of month.
    """
    try:
        today_day = nl_now().day
        tot_d     = total_days()
        days_rem  = days_remaining()

        if days_rem <= 3:
            return current_tot
        if today_day == 0 or current_tot <= 0:
            return None

        daily_rate = current_tot / today_day
        # Linear extrapolation: scale by remaining fraction of month
        linear   = current_tot / (today_day / tot_d)
        # Hard cap: never let forecast exceed current + run-rate * 1.1 for remaining days
        hard_cap = current_tot + days_rem * daily_rate * 1.1

        return round(min(linear, hard_cap))

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
                # Fetch last 5 pages (500 invoices) to capture all recent months
                for pn in range(max(0,lp-4),lp+1):
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
        except Exception as e: print(f"[FETCH invoices] {e}")

        # Also fetch recent placements so job_map covers current-month invoices.
        # Without this, new invoices can't find their job_id and fall to Mireille Prooi.
        try:
            pl_rows=[]
            for page_num in range(0, 5):
                # Fetch placements from last 90 days — not just since last_sync.
                # Invoices can reference placements created months before the invoice date
                # (e.g. a March placement billed in May). Using last_sync misses those.
                pl_cutoff = (nl_now() - timedelta(days=90)).strftime("%Y-%m-%d")
                q=f"""{{ crJobPage(qualifier: "creationDate > (NSCalendarDate) '{pl_cutoff} 00:00:00'",
                          pageable: {{page: {page_num}, size: 100}}) {{
                    totalElements
                    items {{
                        _id startDate creationDate additionalInfo
                        toEmployee {{ firstName lastName }} toCompany {{ name }} agency {{ name }}
                        toVacancy {{ _id creationDate }} toMatch {{ _id creationDate }}
                    }}
                }} }}"""
                data=run_query(q)
                page=(data.get("data",{}).get("crJobPage") or {})
                items=page.get("items",[])
                if not items: break
                for p_item in items:
                    info=p_item.get("additionalInfo") or {}
                    vacancy=p_item.get("toVacancy") or {}
                    match=p_item.get("toMatch") or {}
                    pl_rows.append({
                        "placement_id":   str(p_item["_id"]),
                        "start_date":     p_item.get("startDate"),
                        "creation_date":  p_item.get("creationDate"),
                        "candidate_name": f"{(p_item.get('toEmployee') or {}).get('firstName','')} {(p_item.get('toEmployee') or {}).get('lastName','')}".strip(),
                        "company":        (p_item.get("toCompany") or {}).get("name","Unknown"),
                        "agency":         (p_item.get("agency") or {}).get("name","Unknown"),
                        "vacancy_id":     str(vacancy.get("_id","")),
                        "vacancy_created":vacancy.get("creationDate"),
                        "match_id":       str(match.get("_id","")),
                        "match_created":  match.get("creationDate"),
                        "fase_6525":      str(info.get("_6525","")),
                        "fase_6526":      str(info.get("_6526","")),
                        "fase_6527":      str(info.get("_6527","")),
                        "fase_6528":      str(info.get("_6528","")),
                        "fase_6538":      str(info.get("_6538","")),
                    })
                if len(pl_rows) >= page.get("totalElements",0): break
                _time.sleep(0.3)
            if pl_rows:
                new_pl=pd.DataFrame(pl_rows)
                pl=pd.concat([pl,new_pl],ignore_index=True).drop_duplicates(subset=["placement_id"],keep="last")
                print(f"[FETCH placements] +{len(pl_rows)} → total {len(pl)}")
        except Exception as e: print(f"[FETCH placements] {e}")

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
        iid=str(i.get("invoice_id") or i.get("internal_id") or "")
        for c in cons:
            rows.append({"invoice_id":iid,"consultant":c,"amount":adj,"revenue":rp,
                "date":date,"month":month})
    return pd.DataFrame(rows), forecast, daily_days, daily_rev

def classify_match_stage(row):
    """Classify match into funnel stage — same logic as internal dashboard."""
    sn = str(row.get("status_display") or row.get("status_name") or "").strip()
    if row.get("has_job") or row.get("is_successfully_filled"): return "5. Geplaatst"
    if any(sn.startswith(x) for x in ["5.1","5.2","5.3","5.4","5.5","5.6"]): return "5. Geplaatst"
    if any(sn.startswith(x) for x in ["5.0","6.6","6.7"]): return "4. Aanbod"
    if any(sn.startswith(x) for x in ["3.","4.","6.3","6.4","6.5","6.8","6.9"]): return "3. 1e gesprek klant"
    return "2. Op gesprek"

STAGE_RANK = {"2. Op gesprek":1,"3. 1e gesprek klant":2,"4. Aanbod":3,"5. Geplaatst":4}

@st.cache_data(ttl=7200, show_spinner=False)
def load_pipeline():
    """Load pipeline from matches parquet using proven stage classification."""
    s={"op_gesprek":0,"eerste_gesprek":0,"aanbod":0,"geplaatst":0,"avg_ttf":None,"avg_tth":None}
    try:
        matches = pd.read_parquet(os.path.join(DATA_DIR,"matches.parquet"))
        matches["creation_date"] = pd.to_datetime(matches.get("creation_date",""), errors="coerce")
        matches["month"] = matches["creation_date"].dt.strftime("%Y-%m")

        # Stage funnel = active pipeline regardless of when the match was created.
        # Use rolling 90-day window so old closed matches don't inflate counts.
        cutoff = nl_now() - timedelta(days=90)
        active_m = matches[matches["creation_date"] >= cutoff].copy()

        # Fall back to previous month if current window is empty (parquet not yet synced)
        if active_m.empty:
            active_m = matches[matches["month"].isin([CURRENT_MONTH, PREV_MONTH])].copy()

        if not active_m.empty:
            # Per candidate take highest stage reached
            active_m["stage"] = active_m.apply(classify_match_stage, axis=1)
            active_m["stage_rank"] = active_m["stage"].map(STAGE_RANK).fillna(0).astype(int)
            best = active_m.loc[active_m.groupby("candidate_id")["stage_rank"].idxmax()]
            stage_counts = best["stage"].value_counts()
            s["op_gesprek"]    = int(stage_counts.get("2. Op gesprek", 0))
            s["eerste_gesprek"]= int(stage_counts.get("3. 1e gesprek klant", 0))
            s["aanbod"]        = int(stage_counts.get("4. Aanbod", 0))
            s["geplaatst"]     = int(stage_counts.get("5. Geplaatst", 0))

        # Placements parquet for geplaatst + TTF/TTH — rolling quarter (same window as funnel)
        pl = pd.read_parquet(os.path.join(DATA_DIR,"placements.parquet"))
        pl["creation_date"]  = pd.to_datetime(pl.get("creation_date",""), errors="coerce")
        pl["vacancy_created"]= pd.to_datetime(pl.get("vacancy_created",""), errors="coerce")
        pl["match_created"]  = pd.to_datetime(pl.get("match_created",""), errors="coerce")
        quarter_cutoff = nl_now() - timedelta(days=90)
        cur_pl = pl[pl["creation_date"] >= quarter_cutoff].copy()
        # Fall back to current month only if quarter is somehow empty
        if cur_pl.empty:
            pl["month"] = pl["creation_date"].dt.strftime("%Y-%m")
            cur_pl = pl[pl["month"] == CURRENT_MONTH].copy()
        if not cur_pl.empty:
            s["geplaatst"] = max(s["geplaatst"], len(cur_pl))
            ttf = (cur_pl["creation_date"]-cur_pl["vacancy_created"]).dt.days.dropna()
            tth = (cur_pl["creation_date"]-cur_pl["match_created"]).dt.days.dropna()
            ttf = ttf[(ttf>0)&(ttf<500)]
            tth = tth[(tth>0)&(tth<500)]
            if not ttf.empty: s["avg_ttf"] = round(ttf.mean())
            if not tth.empty: s["avg_tth"] = round(tth.mean())

    except Exception as e: print(f"[PIPELINE] {e}")
    return s

def fetch_vacancy_description(vacancy_id: str) -> str:
    """Fetch intro + requirements text for a single vacancy from Carerix API."""
    import re as _re
    try:
        q = f"""{{ crVacancy(_id: "{vacancy_id}") {{
            introInformation additionalInformation requirements
        }} }}"""
        data = run_query(q)
        v = (data.get("data") or {}).get("crVacancy") or {}
        parts = [v.get("introInformation") or "", v.get("additionalInformation") or "", v.get("requirements") or ""]
        text = " ".join(p for p in parts if p and str(p) not in ("None","null",""))
        text = _re.sub(r'<[^>]+>', ' ', text)
        text = _re.sub(r'\s+', ' ', text).strip()
        return text[:250] + "…" if len(text) > 250 else text
    except Exception as e:
        print(f"[VAC_DESC] {vacancy_id}: {e}"); return ""

@st.cache_data(ttl=7200, show_spinner=False)
def load_vacancies():
    """Load active vacancies — one per recruiter so the carousel shows variety."""
    try:
        vac_raw = pd.read_parquet(os.path.join(DATA_DIR,"vacancies.parquet"))
        active = vac_raw[vac_raw["status"].isin(ACTIVE_STATUSES)].copy()
        active = active[active["job_title"].notna() & ~active["job_title"].isin(["--","None","nan",""])]
        total_count = len(active)

        # Exclude internal/backoffice accounts
        active = active[~active["owner"].astype(str).str.lower().str.contains("backoffice|lincks backoffice", na=False)]
        active = active.sort_values("creation_date", ascending=False)
        active["_owner_key"] = active["owner"].astype(str).str.strip().str.lower()
        one_per = active.drop_duplicates(subset=["_owner_key"], keep="first")
        one_per = one_per.drop_duplicates(subset=["job_title"], keep="first")
        selection = one_per.head(10)

        result = []
        for _, v in selection.iterrows():
            owner_parts = str(v.get("owner","")).split(" ")
            vacancy_id  = str(v.get("vacancy_id",""))
            # Fetch real description from Carerix API
            intro_clean = fetch_vacancy_description(vacancy_id) if vacancy_id and CLIENT_SECRET else ""
            match_count = int(v.get("match_count", 0) or 0)
            agency      = str(v.get("agency",""))
            result.append({
                "_id":          vacancy_id,
                "jobTitle":     str(v.get("job_title","")),
                "vacancyNo":    str(v.get("vacancy_no","")),
                "creationDate": str(v.get("creation_date","")),
                "statusDisplay":str(v.get("status","")),
                "toCompany":    {"name": str(v.get("company","—"))},
                "owner":        {"firstName": owner_parts[0] if owner_parts else "",
                                 "lastName":  " ".join(owner_parts[1:]) if len(owner_parts)>1 else ""},
                "workLocation": str(v.get("work_location", v.get("workLocation",""))),
                "intro":        intro_clean,
                "match_count":  match_count,
                "agency":       agency,
            })
        return result, total_count
    except Exception as e:
        print(f"[VACANCIES] parquet failed: {e}")
        return [], 0

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
# Deduplicate on invoice_id for company total (matches Final dashboard logic)
# An invoice split across 2 consultants has 2 rows with same invoice_id — only count amount once
tot=df_cur.drop_duplicates(subset=["invoice_id"])["amount"].sum()
# Compare vs same day-of-month in previous month (not the full prev month total)
today_day = nl_now().day
df_prev_ytd = df_prev[df_prev["date"].dt.day <= today_day].drop_duplicates(subset=["invoice_id"])
prev_tot_ytd = df_prev_ytd["amount"].sum()
pct=min(tot/COMPANY_TARGET*100,100)
rem=max(COMPANY_TARGET-tot,0)
delta=tot-prev_tot_ytd
dpc=(delta/prev_tot_ytd*100) if prev_tot_ytd>0 else 0
EXCLUDE_CONSULTANTS = ["Mireille Prooi","Onbekend","Backoffice Lincks","Backoffice"]
excl=df_cur[~df_cur["consultant"].isin(EXCLUDE_CONSULTANTS)]
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

# Auto-rerun every 10s so the 60s screen-switch and 6s vacancy-cycle checks fire on time
st_autorefresh(interval=10_000, limit=None, key="tv_autorefresh")

now_t=_time.time()
if not st.session_state["locked"] and now_t-st.session_state["last_sw"]>60:
    st.session_state["screen"]=(st.session_state["screen"]+1)%3
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
        if st.button("★  Vacatures", key="b2"):
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
                showlegend=False,margin=dict(l=10,r=10,t=10,b=10),height=220)
            st.plotly_chart(fig,use_container_width=True)

        with cb:
            # Revenue chart with forecast line
            if daily_days and daily_rev:
                all_days=list(range(1,tot_days+1))
                # Actual line up to today
                act_x=daily_days; act_y=daily_rev
                # Forecast curve: run model for each future day using cumulative revenue
                # at that point, giving a realistic curved path rather than a straight line.
                fc_x, fc_y = [], []
                if forecast and len(act_y) > 0:
                    inv_raw_fc = pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet"))
                    last_day   = daily_days[-1]
                    last_val   = act_y[-1]
                    rem_days   = tot_days - last_day
                    if rem_days > 0:
                        # Distribute remaining revenue via empirical intramonth shape:
                        # compute what fraction of a month falls in each remaining day,
                        # then scale so total matches the model's end-of-month forecast.
                        hist_df = inv_raw_fc.copy()
                        hist_df["_date"]  = pd.to_datetime(hist_df.get("date", hist_df.get("value_date","")), errors="coerce")
                        hist_df["_month"] = hist_df["_date"].dt.strftime("%Y-%m")
                        hist_df["_day"]   = hist_df["_date"].dt.day
                        hist_df["_total"] = pd.to_numeric(hist_df.get("total",0), errors="coerce").fillna(0)
                        hist_df["_status"]= hist_df.get("status","").astype(str)
                        hist_past = hist_df[(hist_df["_status"]=="Verzonden") & (hist_df["_month"]!=CURRENT_MONTH)]
                        # Average daily revenue as fraction of month total across history
                        day_weights = {}
                        for m in hist_past["_month"].unique():
                            mdf = hist_past[hist_past["_month"]==m]
                            mt  = mdf["_total"].sum()
                            if mt <= 0: continue
                            for d, g in mdf.groupby("_day"):
                                day_weights[d] = day_weights.get(d, []) + [g["_total"].sum() / mt]
                        avg_day_frac = {d: float(np.mean(v)) for d, v in day_weights.items()}
                        # Sum of weights for remaining days
                        future_days = list(range(last_day + 1, tot_days + 1))
                        raw_w = [avg_day_frac.get(d, 0.001) for d in future_days]
                        total_rem_frac = sum(raw_w) if sum(raw_w) > 0 else 1
                        remaining_revenue = forecast - last_val
                        cumulative = last_val
                        fc_x = [last_day]
                        fc_y = [last_val]
                        for i, d in enumerate(future_days):
                            cumulative += remaining_revenue * (raw_w[i] / total_rem_frac)
                            fc_x.append(d)
                            fc_y.append(round(cumulative))

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
        rc_df = df_cur[df_cur["consultant"] != "Mireille Prooi"].groupby("consultant")["revenue"].sum().reset_index()
        def get_target_bar(name):
            if name in TARGETS: return TARGETS[name]
            last = name.split()[-1] if name else ""
            for k, v in TARGETS.items():
                if k.split()[-1] == last: return v
            return DEFAULT_TARGET
        rc_df["target"]    = rc_df["consultant"].apply(get_target_bar)
        rc_df["pct"]       = (rc_df["revenue"] / rc_df["target"] * 100).round(1)
        rc_df["resterend"] = (rc_df["target"] - rc_df["revenue"]).clip(lower=0)
        rc_df              = rc_df.sort_values("revenue", ascending=True)
        rc_df["color"]     = rc_df["pct"].apply(lambda x: "#00e5a0" if x >= 100 else ("#f5a623" if x >= 80 else "#e92076"))
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=rc_df["revenue"], y=rc_df["consultant"], orientation="h", name="Behaald",
            marker_color=rc_df["color"], marker_line_width=0,
            text=rc_df.apply(lambda r: f"€{r['revenue']:,.0f}  ({r['pct']:.0f}%)", axis=1),
            textposition="inside", insidetextanchor="middle",
            textfont=dict(color="white", size=12),
            customdata=rc_df[["target","pct"]].values,
            hovertemplate="<b>%{y}</b><br>Behaald: €%{x:,.0f}<br>Target: €%{customdata[0]:,.0f}<br>%{customdata[1]:.0f}%<extra></extra>"
        ))
        fig3.add_trace(go.Bar(
            x=rc_df["resterend"], y=rc_df["consultant"], orientation="h", name="Resterend",
            marker_color="rgba(255,255,255,0.05)",
            marker_line_color="rgba(255,255,255,0.1)", marker_line_width=1,
            hovertemplate="<b>%{y}</b><br>Nog te gaan: €%{x:,.0f}<extra></extra>"
        ))
        fig3.update_layout(
            barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.5)",
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickprefix="€", tickfont=dict(size=10), zeroline=False),
            yaxis=dict(tickfont=dict(size=13), gridcolor="rgba(0,0,0,0)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
                font=dict(color="rgba(255,255,255,0.35)", size=10)),
            margin=dict(l=0, r=10, t=30, b=0), height=300, bargap=0.22
        )
        st.plotly_chart(fig3, use_container_width=True)

    # ── PIPELINE ─────────────────────────────────────────────────────────────
    elif scr==1:
        p=pipeline
        ttf=f"{p['avg_ttf']} dgn" if p['avg_ttf'] else "—"
        tth=f"{p['avg_tth']} dgn" if p['avg_tth'] else "—"

        # Quarter period label: 90 days back from today
        q_start = (nl_now() - timedelta(days=90)).strftime("%-d %B")
        q_end   = nl_now().strftime("%-d %B %Y")
        NL_MONTHS = {"January":"januari","February":"februari","March":"maart","April":"april",
                     "May":"mei","June":"juni","July":"juli","August":"augustus",
                     "September":"september","October":"oktober","November":"november","December":"december"}
        for en,nl in NL_MONTHS.items():
            q_start=q_start.replace(en,nl); q_end=q_end.replace(en,nl)
        st.markdown(f"""<div style="font-size:0.58rem;color:rgba(255,255,255,0.2);letter-spacing:3px;
            text-transform:uppercase;margin-bottom:1rem;">
            Kwartaal · {q_start} – {q_end}
        </div>""", unsafe_allow_html=True)

        kpis=[
            ("Op Gesprek",      p["op_gesprek"],      "#00d4c8"),
            ("1e Gesprek Klant",p["eerste_gesprek"],  "#e92076"),
            ("Heeft Aanbod",    p["aanbod"],           "#a78bfa"),
            ("Geplaatst ✓",     p["geplaatst"],        "#00e5a0"),
        ]
        cols=st.columns(4)
        for col,(lbl,val,clr) in zip(cols,kpis):
            with col:
                st.markdown(f"""<div class="fkpi">
                    <div class="fkpi-val" style="color:{clr}">{val}</div>
                    <div class="fkpi-lbl">{lbl}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        cf,ct=st.columns([1.4,1])
        with cf:
            lbls=["Op Gesprek","1e Gesprek Klant","Aanbod","Geplaatst"]
            vals=[p["op_gesprek"],p["eerste_gesprek"],p["aanbod"],p["geplaatst"]]
            clrs=["#00d4c8","#e92076","#a78bfa","#00e5a0"]
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
                <div class="card-label">vs vorige maand (t/m dag {today_day})</div>
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
            title    = v.get("jobTitle","—")
            company  = (v.get("toCompany") or {}).get("name","—")
            ow       = v.get("owner") or {}
            cons     = f"{ow.get('firstName','')} {ow.get('lastName','')}".strip() or "—"
            created  = pd.to_datetime(v.get("creationDate")) if v.get("creationDate") else None
            days_open= (nl_now()-created.replace(tzinfo=None)).days if created else 0
            loc      = v.get("workLocation","") or ""
            if loc in ("None","nan"): loc=""
            vno      = v.get("vacancyNo","")
            status   = v.get("statusDisplay","")
            intro       = v.get("intro","") or ""
            match_count = v.get("match_count", 0)
            agency      = v.get("agency","") or ""
            if agency in ("None","nan"): agency=""

            days_c="#e92076" if days_open>60 else "#f5a623" if days_open>30 else "#00d4c8"
            pills=""
            if loc and loc not in ("None","nan",""): pills+=f'<span class="pill">📍 {loc}</span>'
            if status: pills+=f'<span class="pill">{status}</span>'

            dots="".join([f'<div class="dot {"on" if i==idx else ""}"></div>' for i in range(len(vacs))])

            # Top row: big total count card + progress indicator
            ch,_sp=st.columns([1,3])
            with ch:
                st.markdown(f"""
                <a href="https://www.lincks.nl/vacatures" target="_blank" style="text-decoration:none;">
                  <div style="background:linear-gradient(135deg,rgba(233,32,118,0.15),rgba(0,212,200,0.08));
                    border:1px solid rgba(233,32,118,0.3);border-radius:14px;padding:0.9rem 1.4rem;
                    cursor:pointer;transition:all 0.2s;display:flex;align-items:center;gap:1rem;">
                    <div>
                      <div style="font-family:Syne,sans-serif;font-size:2.6rem;font-weight:800;
                        color:#e92076;line-height:1;">{open_vac_count}</div>
                      <div style="font-size:0.58rem;color:rgba(255,255,255,0.3);letter-spacing:3px;
                        text-transform:uppercase;margin-top:0.3rem;">open vacatures</div>
                    </div>
                    <div style="font-size:0.7rem;color:rgba(255,255,255,0.2);margin-left:auto;">
                      lincks.nl/vacatures ↗
                    </div>
                  </div>
                </a>
                """, unsafe_allow_html=True)

            st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

            cm,cl=st.columns([1.7,1])
            with cm:
                st.markdown(f"""
                <div class="vac-big">
                  <div class="vac-num">{idx+1} van {len(vacs)} · vacature {vno}</div>
                  <div class="vac-title">{title}</div>
                  <div class="vac-company">🏢 {company}</div>
                  <div class="pills">{pills}</div>
                  <div style="margin:0.8rem 0 1rem;padding:1rem 1.1rem;
                    background:rgba(255,255,255,0.03);border-radius:10px;
                    border-left:2px solid rgba(0,212,200,0.35);">
                    <div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:0.6rem;flex-wrap:wrap;">
                      <span style="font-size:0.95rem;font-weight:600;color:white;">👤 {cons}</span>
                      {"<span style='font-size:0.72rem;color:rgba(255,255,255,0.3);'>· " + agency + "</span>" if agency else ""}
                    </div>
                    <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.7rem;">
                      {"<span style='background:rgba(0,212,200,0.1);border:1px solid rgba(0,212,200,0.25);border-radius:20px;padding:0.2rem 0.7rem;font-size:0.7rem;color:#00d4c8;'>📋 " + str(match_count) + " matches</span>" if match_count else ""}
                      <span style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:20px;padding:0.2rem 0.7rem;font-size:0.7rem;color:rgba(255,255,255,0.45);">#{vno}</span>
                      <span style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:20px;padding:0.2rem 0.7rem;font-size:0.7rem;color:{days_c};">⏱ {days_open} dagen open</span>
                    </div>
                    {("<div style='font-size:0.82rem;color:rgba(255,255,255,0.45);line-height:1.6;'>" + intro + "</div>") if intro else ""}</div>
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
                st.markdown('<div style="font-size:0.52rem;color:rgba(255,255,255,0.18);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.6rem;padding-left:0.3rem">Per recruiter</div>', unsafe_allow_html=True)
                for i,vv in enumerate(vacs):
                    t2  = vv.get("jobTitle","—")
                    c2  = (vv.get("toCompany") or {}).get("name","—")
                    ow2 = vv.get("owner") or {}
                    r2  = f"{ow2.get('firstName','')} {ow2.get('lastName','')}".strip() or "—"
                    cr2 = pd.to_datetime(vv.get("creationDate")) if vv.get("creationDate") else None
                    do2 = (nl_now()-cr2.replace(tzinfo=None)).days if cr2 else 0
                    cls2= "vac-sm cur" if i==idx else "vac-sm"
                    dc2 = "#e92076" if do2>60 else "#f5a623" if do2>30 else "rgba(255,255,255,0.2)"
                    st.markdown(f"""<div class="{cls2}">
                        <div class="vac-sm-t">{t2}</div>
                        <div class="vac-sm-c" style="margin-top:0.15rem;">
                          <span style="color:rgba(255,255,255,0.45);font-weight:500">{r2}</span>
                          · {c2}
                          · <span style="color:{dc2}">{do2}d</span>
                        </div>
                    </div>""", unsafe_allow_html=True)

render_screen()

st.markdown("""
<div style="text-align:center;padding:0.8rem 0 0.3rem;color:rgba(255,255,255,0.05);
    font-size:0.52rem;letter-spacing:3px;text-transform:uppercase">
    Data gecached · vernieuwd elke 2 uur
</div></div>
""", unsafe_allow_html=True)