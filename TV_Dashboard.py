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
html,body,[class*="css"]{font-family:'Inter',sans-serif;background:#180329;color:white;}
.stApp,[data-testid="stAppViewContainer"]{background:#180329;}
.block-container{padding:0!important;max-width:100%!important;}
footer,#MainMenu,header,[data-testid="collapsedControl"]{display:none!important;}
section[data-testid="stSidebar"]{display:none!important;}
.stApp::before{content:'';position:fixed;top:0;left:0;right:0;bottom:0;
  background-image:linear-gradient(rgba(233,32,118,0.04) 1px,transparent 1px),
  linear-gradient(90deg,rgba(233,32,118,0.04) 1px,transparent 1px);
  background-size:60px 60px;pointer-events:none;z-index:0;}
.wrap{position:relative;z-index:1;padding:0.9rem 2.5rem 1rem;}
.hdr{display:flex;align-items:center;margin-bottom:0.7rem;}
.hdr-left{display:flex;align-items:center;gap:1.2rem;}
.hdr-badge{background:#e92076;color:white;font-size:0.55rem;font-weight:700;
  letter-spacing:3px;padding:0.25rem 0.7rem;border-radius:3px;text-transform:uppercase;}
.divider{height:1px;background:rgba(233,32,118,0.15);margin-bottom:0.6rem;}
.card{background:linear-gradient(135deg,#2a0845,#1e0535);border:1px solid rgba(233,32,118,0.3);
  border-radius:14px;padding:1.5rem;position:relative;overflow:hidden;}
.card-top{position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#e92076,#63ccca);}
.card-label{font-size:0.58rem;font-weight:600;color:rgba(255,255,255,0.5);
  text-transform:uppercase;letter-spacing:3px;margin-bottom:0.7rem;}
.card-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.pink{color:#e92076;}.teal{color:#00d4c8;}.green{color:#00e5a0;}.orange{color:#f5a623;}
.card-sub{font-size:0.7rem;color:rgba(255,255,255,0.35);margin-top:0.5rem;}
.prog{height:3px;background:rgba(255,255,255,0.08);border-radius:2px;margin-top:1rem;overflow:hidden;}
.prog-fill{height:100%;background:linear-gradient(90deg,#e92076,#63ccca);border-radius:2px;}
.fkpi{background:linear-gradient(135deg,#2a0845,#1e0535);border:1px solid rgba(233,32,118,0.3);
  border-radius:12px;padding:1.5rem 1rem;text-align:center;}
.fkpi-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.fkpi-lbl{font-size:0.58rem;color:rgba(255,255,255,0.5);letter-spacing:2px;
  text-transform:uppercase;margin-top:0.5rem;}

/* Vacancy carousel */
.vac-stage{position:relative;width:100%;overflow:hidden;border-radius:18px;}
.vac-big{
  background:linear-gradient(145deg,#1e0535 0%,#2a0845 60%,#180329 100%);
  border:1px solid rgba(233,32,118,0.3);border-radius:18px;
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
  margin-bottom:1.2rem;max-height:120px;overflow:hidden;}
.pills{display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem;}
.pill{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);
  border-radius:20px;padding:0.3rem 0.85rem;font-size:0.72rem;color:rgba(255,255,255,0.55);}
.pill.acc{background:rgba(0,212,200,0.1);border-color:rgba(0,212,200,0.25);color:#00d4c8;}
.vac-recruiter{font-size:0.7rem;color:rgba(255,255,255,0.3);margin-top:0.8rem;}
.dots{display:flex;gap:0.35rem;justify-content:center;margin-top:1rem;}
.dot{width:5px;height:5px;border-radius:50%;background:rgba(255,255,255,0.1);}
.dot.on{background:#e92076;width:18px;border-radius:3px;}

/* Vacancy list */
.vac-sm{background:rgba(42,8,69,0.5);border:1px solid rgba(233,32,118,0.15);
  border-left:2px solid rgba(233,32,118,0.4);border-radius:8px;
  padding:0.65rem 0.9rem;margin-bottom:0.4rem;}
.vac-sm.cur{border-left-color:#63ccca;background:rgba(99,204,202,0.05);}
.vac-sm-t{font-size:0.8rem;font-weight:600;color:white;margin-bottom:0.1rem;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.vac-sm-c{font-size:0.65rem;color:rgba(255,255,255,0.4);}

/* Nav tabs */
.nav-tabs{display:flex;gap:0.4rem;margin-bottom:1.4rem;align-items:center;}
.stButton>button{
  background:rgba(42,8,69,0.6)!important;color:rgba(255,255,255,0.4)!important;
  border:1px solid rgba(233,32,118,0.2)!important;border-radius:6px!important;
  font-size:0.62rem!important;letter-spacing:2px!important;text-transform:uppercase!important;
  font-weight:500!important;padding:0.3rem 0.75rem!important;transition:all 0.15s!important;
}
.stButton>button:hover{
  background:rgba(233,32,118,0.15)!important;
  border-color:rgba(233,32,118,0.4)!important;
  color:white!important;
}
.stSelectbox>div>div{background:#2a0845!important;
  border:1px solid rgba(233,32,118,0.3)!important;color:white!important;
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
    "Arjan Huisman":35000,"Dico Cabout":31250,
    "Sharon Kruijssen":31250,"Rick Wit":15000,
    "Esther Moerman":16667,"Nina Harten":15000,
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
    AR(3) + multiplicative seasonality + intramonth CDF blend.

    Components:
      1. AR(3) baseline  — OLS fit on monthly totals; gives a historically-grounded
                           anchor independent of how the current month started.
      2. Seasonal factor — ratio of same-calendar-month historical average vs overall
                           monthly average; adjusts for May being stronger/weaker.
      3. Intramonth CDF  — empirical median fraction received by day D across historical
                           months.  Captures the end-of-month invoice rush: day 7 might
                           historically represent only ~10% of a month, so we don't
                           extrapolate linearly.
      4. Blend           — early in month: AR(3) dominates (actual data is scarce);
                           late in month: intramonth CDF dominates (we can see the shape).
    """
    try:
        today_day  = nl_now().day
        cal_month  = nl_now().month
        days_rem   = days_remaining()
        tot_d      = total_days()
        progress   = today_day / tot_d          # 0..1

        df = inv_raw.copy()
        df["_date"]  = pd.to_datetime(df.get("date", df.get("value_date", "")), errors="coerce")
        df = df[df["_date"].notna()].copy()
        df["_month"] = df["_date"].dt.strftime("%Y-%m")
        df["_day"]   = df["_date"].dt.day
        df["_total"] = pd.to_numeric(df.get("total", 0), errors="coerce").fillna(0)
        df["_status"]= df.get("status", "").astype(str)
        hist = df[(df["_status"] == "Verzonden") & (df["_month"] != CURRENT_MONTH)]
        if hist.empty: return None

        # ── 1.  Monthly totals sorted chronologically ─────────────────────────
        monthly = hist.groupby("_month")["_total"].sum().sort_index()
        months  = list(monthly.index)
        totals  = list(monthly.values)
        if len(totals) < 2: return None

        # ── 2.  AR(3) via OLS (fall back to weighted mean if too few months) ──
        ar3_pred = None
        if len(totals) >= 5:
            p = min(3, len(totals) - 2)          # lags actually used
            X, y = [], []
            for i in range(p, len(totals)):
                X.append([1.0] + [totals[i-k] for k in range(1, p+1)])
                y.append(totals[i])
            X, y = np.array(X), np.array(y)
            try:
                coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
                ar3_pred = coeffs[0] + sum(coeffs[k+1] * totals[-(k+1)] for k in range(p))
                ar3_pred = max(ar3_pred, 0)
            except Exception:
                pass

        if ar3_pred is None:
            # Weighted average of last 3 months as fallback
            w = np.array([0.5, 0.3, 0.2][:len(totals)])
            w /= w.sum()
            ar3_pred = float(np.dot(w, totals[-len(w):]))

        # ── 3.  Seasonal multiplier ───────────────────────────────────────────
        same_cal   = [t for m, t in zip(months, totals) if int(m.split("-")[1]) == cal_month]
        overall_avg= np.mean(totals)
        if same_cal and overall_avg > 0:
            seasonal = np.mean(same_cal) / overall_avg
            seasonal = np.clip(seasonal, 0.5, 2.0)   # guard against outliers
        else:
            seasonal = 1.0

        ar3_seasonal = ar3_pred * seasonal

        # ── 4.  Intramonth CDF extrapolation ─────────────────────────────────
        # For each historical month collect: fraction of total revenue received by day D.
        # Weighting: same calendar-month counts double (stronger seasonal signal).
        fracs = []
        weights = []
        for m in monthly.index:
            mdf = hist[hist["_month"] == m]
            mt  = mdf["_total"].sum()
            if mt <= 0: continue
            frac = mdf[mdf["_day"] <= today_day]["_total"].sum() / mt
            if not (0 < frac <= 1): continue
            w = 2.0 if int(m.split("-")[1]) == cal_month else 1.0
            fracs.append(frac)
            weights.append(w)

        intra_pred = None
        if fracs:
            # Weighted median (more robust than mean for skewed end-of-month distributions)
            fracs_arr   = np.array(fracs)
            weights_arr = np.array(weights) / np.sum(weights)
            sorted_idx  = np.argsort(fracs_arr)
            csw = np.cumsum(weights_arr[sorted_idx])
            median_frac = float(fracs_arr[sorted_idx[np.searchsorted(csw, 0.5)]])
            if median_frac > 0.02:
                intra_pred = current_tot / median_frac

        # ── 5.  Blend AR(3) + intramonth ─────────────────────────────────────
        # Early in month (day 1-7): AR(3)+seasonality dominates because we have little
        # actual data and the first few days are noisy.
        # Late in month (day 20+): intramonth CDF is reliable; trust it more.
        if intra_pred is not None and ar3_seasonal > 0:
            intra_w = np.clip(0.15 + progress * 0.85, 0.15, 0.9)
            ar3_w   = 1.0 - intra_w
            blended = ar3_w * ar3_seasonal + intra_w * intra_pred
        elif intra_pred is not None:
            blended = intra_pred
        else:
            blended = ar3_seasonal

        # ── 6.  Sanity bounds ─────────────────────────────────────────────────
        if days_rem <= 3:
            return round(current_tot * (1 + 0.008 * days_rem))

        hist_p90   = np.percentile(totals, 90) if totals else COMPANY_TARGET
        max_cap    = max(current_tot, min(hist_p90 * 1.3,
                         current_tot + (days_rem / tot_d) * COMPANY_TARGET))
        blended    = np.clip(blended, current_tot, max_cap)

        return round(blended)

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
                # Fetch placements from last 180 days — invoices can reference placements
                # created many months before the invoice date. The parquet covers up to
                # last_sync; this fetch covers last_sync → today plus a wide overlap buffer.
                pl_cutoff = (nl_now() - timedelta(days=180)).strftime("%Y-%m-%d")
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

    # Forecast is computed OUTSIDE this cached function using the deduplicated tot
    # (invoice split across 2 consultants = 2 rows; daily_rev double-counts it)

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
    mp_amt = sum(r["amount"] for r in rows if r["consultant"]=="Mireille Prooi" and r["month"]==CURRENT_MONTH)
    real_amt = sum(r["amount"] for r in rows if r["consultant"]!="Mireille Prooi" and r["month"]==CURRENT_MONTH)
    print(f"[JOB_MAP] current month: attributed={real_amt:.0f} fallback_MP={mp_amt:.0f} placements_in_map={len(job_map)}")
    return pd.DataFrame(rows), daily_days, daily_rev

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
        return text[:900] + "…" if len(text) > 900 else text
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

        # Exclude internal/backoffice accounts and former employees
        active = active[~active["owner"].astype(str).str.lower().str.contains("backoffice|lincks backoffice|birgit lucas|kristen snel|kirsten stel", na=False)]
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
try:
    df,daily_days,daily_rev=load_invoice_df()
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

# Always compute forecast fresh with deduplicated tot — not inside the cached function
# (cached function used non-deduped daily_rev which double-counts split invoices)
forecast=compute_forecast(pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet")), tot)

# ── Data refresh only — screen/vacancy switching fully in JavaScript ──────────
st_autorefresh(interval=300_000, limit=None, key="tv_autorefresh")

import plotly.io as pio

# ── Logo ──────────────────────────────────────────────────────────────────────
logo_path=os.path.join(os.path.dirname(os.path.abspath(__file__)),"lincks_logo_fc-wit_def.png")
if os.path.exists(logo_path):
    with open(logo_path,"rb") as f: lb64=base64.b64encode(f.read()).decode()
    logo_html=f'<img src="data:image/png;base64,{lb64}" style="height:40px;" />'
else:
    logo_html='<span style="font-family:Syne,sans-serif;font-size:1.4rem;font-weight:800;">LINCKS</span>'

# ── Helper: smart sentence-aware truncation ───────────────────────────────────
def smart_truncate(text, max_chars=650):
    if not text or len(text) <= max_chars:
        return text
    chunk = text[:max_chars]
    last_end = max(chunk.rfind('.'), chunk.rfind('!'), chunk.rfind('?'))
    if last_end > max_chars // 3:
        return chunk[:last_end + 1]
    last_space = chunk.rfind(' ')
    return (chunk[:last_space] + '…') if last_space > 0 else chunk + '…'

# ── Helper: figure → embeddable HTML ─────────────────────────────────────────
_plotly_loaded = False
def ch(fig):
    global _plotly_loaded
    js = 'cdn' if not _plotly_loaded else False
    _plotly_loaded = True
    return pio.to_html(fig, full_html=False, include_plotlyjs=js,
                       config={'responsive': True, 'displayModeBar': False})

# ══════════════════════════════════════════════════════
# BUILD SCREEN 0 — OMZET
# ══════════════════════════════════════════════════════
dc = "#00e5a0" if days_rem<=3 else "#f5a623" if days_rem<=7 else "#00d4c8"
if forecast and forecast > 0:
    fp = min(forecast / COMPANY_TARGET * 100, 150)
    fc = "#00e5a0" if fp>=100 else "#f5a623" if fp>=80 else "#e92076"
    ft = f"€{forecast:,.0f}"
    fs = f"{fp:.0f}% van target · historisch dagpatroon"
else:
    fc = "rgba(255,255,255,0.25)"; ft = "—"; fs = "onvoldoende data"

# Donut
_fig_donut = go.Figure(go.Pie(values=[tot,rem], labels=["Behaald","Resterend"],
    hole=0.8, sort=False, textinfo="none",
    marker_colors=["#e92076","rgba(255,255,255,0.04)"], marker=dict(line=dict(width=0))))
_fig_donut.add_annotation(text=f"<b>{pct:.0f}%</b>", x=0.5, y=0.58,
    font=dict(size=56, color="#e92076", family="Syne"), showarrow=False)
_fig_donut.add_annotation(text=f"€{tot:,.0f}", x=0.5, y=0.40,
    font=dict(size=15, color="rgba(255,255,255,0.6)"), showarrow=False)
_fig_donut.add_annotation(text=f"van €{COMPANY_TARGET:,.0f}", x=0.5, y=0.27,
    font=dict(size=10, color="rgba(255,255,255,0.22)"), showarrow=False)
_fig_donut.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
    showlegend=False, margin=dict(l=10,r=10,t=10,b=10), height=220, autosize=True)
_donut_h = ch(_fig_donut)

# Revenue / forecast line or fallback bar
if daily_days and daily_rev:
    act_x=daily_days; act_y=daily_rev
    fc_x, fc_y = [], []
    if forecast and len(act_y) > 0:
        inv_raw_fc = pd.read_parquet(os.path.join(DATA_DIR,"invoices.parquet"))
        last_day = daily_days[-1]; last_val = act_y[-1]
        if tot_days - last_day > 0:
            hist_df = inv_raw_fc.copy()
            hist_df["_date"]  = pd.to_datetime(hist_df.get("date", hist_df.get("value_date","")), errors="coerce")
            hist_df["_month"] = hist_df["_date"].dt.strftime("%Y-%m")
            hist_df["_day"]   = hist_df["_date"].dt.day
            hist_df["_total"] = pd.to_numeric(hist_df.get("total",0), errors="coerce").fillna(0)
            hist_df["_status"]= hist_df.get("status","").astype(str)
            hist_past = hist_df[(hist_df["_status"]=="Verzonden") & (hist_df["_month"]!=CURRENT_MONTH)]
            day_weights = {}
            for _m in hist_past["_month"].unique():
                _mdf = hist_past[hist_past["_month"]==_m]; _mt = _mdf["_total"].sum()
                if _mt <= 0: continue
                for _d, _g in _mdf.groupby("_day"):
                    day_weights[_d] = day_weights.get(_d, []) + [_g["_total"].sum() / _mt]
            avg_day_frac = {_d: float(np.mean(_v)) for _d, _v in day_weights.items()}
            future_days = list(range(last_day+1, tot_days+1))
            raw_w = [avg_day_frac.get(_d, 0.001) for _d in future_days]
            total_rem_frac = sum(raw_w) if sum(raw_w) > 0 else 1
            cumulative = last_val; fc_x = [last_day]; fc_y = [last_val]
            for _i, _d in enumerate(future_days):
                cumulative += (forecast - last_val) * (raw_w[_i] / total_rem_frac)
                fc_x.append(_d); fc_y.append(round(cumulative))
    _fig_rev = go.Figure()
    _fig_rev.add_trace(go.Scatter(x=act_x, y=act_y, mode="lines", name="Werkelijk",
        line=dict(color="#e92076", width=3), fill="tozeroy", fillcolor="rgba(233,32,118,0.08)"))
    if fc_x:
        _fig_rev.add_trace(go.Scatter(x=fc_x, y=fc_y, mode="lines", name="Verwachting",
            line=dict(color="#f5a623", width=2, dash="dot")))
        _fig_rev.add_hline(y=COMPANY_TARGET, line=dict(color="rgba(255,255,255,0.15)",width=1,dash="dash"),
            annotation_text="Target", annotation_font=dict(color="rgba(255,255,255,0.3)",size=10))
    _fig_rev.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="rgba(255,255,255,0.4)",
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickvals=list(range(1,tot_days+1,5)),
            title="Dag", tickfont=dict(size=10)),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickprefix="€", tickfont=dict(size=10)),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
            font=dict(color="rgba(255,255,255,0.4)", size=10)),
        margin=dict(l=0,r=10,t=30,b=0), height=240, autosize=True)
    _right_h = ch(_fig_rev)
else:
    def _get_t(n):
        if n in TARGETS: return TARGETS[n]
        last=n.split()[-1] if n else ""
        for k,v in TARGETS.items():
            if k.split()[-1]==last: return v
        return DEFAULT_TARGET
    _rc = excl.groupby("consultant")["revenue"].sum().reset_index()
    _rc["t"] = pd.to_numeric(_rc["consultant"].apply(_get_t), errors="coerce").fillna(DEFAULT_TARGET)
    _rc["p"] = (_rc["revenue"]/_rc["t"]*100).round(1)
    _rc["r"] = (_rc["t"]-_rc["revenue"]).clip(lower=0)
    _rc = _rc.sort_values("revenue", ascending=True)
    _rc["c"] = _rc["p"].apply(lambda x: "#00e5a0" if x>=100 else ("#f5a623" if x>=80 else "#e92076"))
    _fig_fb = go.Figure()
    _fig_fb.add_trace(go.Bar(x=_rc["revenue"], y=_rc["consultant"], orientation="h",
        marker_color=_rc["c"], marker_line_width=0,
        text=_rc.apply(lambda r: f"  €{r['revenue']:,.0f}  ·  {r['p']:.0f}%", axis=1),
        textposition="inside", insidetextanchor="start", textfont=dict(color="white",size=11,family="Inter")))
    _fig_fb.add_trace(go.Bar(x=_rc["r"], y=_rc["consultant"], orientation="h",
        marker_color="rgba(255,255,255,0.03)", marker_line_color="rgba(255,255,255,0.06)", marker_line_width=1))
    _fig_fb.update_layout(barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_color="rgba(255,255,255,0.4)",
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickprefix="€", tickfont=dict(size=10), zeroline=False),
        yaxis=dict(tickfont=dict(size=12,family="Inter"), gridcolor="rgba(0,0,0,0)"),
        margin=dict(l=0,r=10,t=10,b=0), height=300, bargap=0.2, showlegend=False, autosize=True)
    _right_h = ch(_fig_fb)

# Recruiter target bar (shared across screen 0)
def _get_tbar(name):
    if name in TARGETS: return TARGETS[name]
    last = name.split()[-1] if name else ""
    for k,v in TARGETS.items():
        if k.split()[-1]==last: return v
    return DEFAULT_TARGET
_rc_df = df_cur[df_cur["consultant"]!="Mireille Prooi"].groupby("consultant")["revenue"].sum().reset_index()
_existing = set(_rc_df["consultant"].tolist())
_zero_rows = [{"consultant":n,"revenue":0.0} for n,t in TARGETS.items() if t>0 and n not in _existing]
if _zero_rows: _rc_df = pd.concat([_rc_df, pd.DataFrame(_zero_rows)], ignore_index=True)
_rc_df["target"]    = _rc_df["consultant"].apply(_get_tbar)
_rc_df["pct"]       = (_rc_df["revenue"]/_rc_df["target"]*100).round(1)
_rc_df["resterend"] = (_rc_df["target"]-_rc_df["revenue"]).clip(lower=0)
_rc_df              = _rc_df.sort_values("revenue", ascending=True)
_rc_df["color"]     = _rc_df["pct"].apply(lambda x: "#00e5a0" if x>=100 else ("#f5a623" if x>=80 else "#e92076"))
_fig3 = go.Figure()
_fig3.add_trace(go.Bar(x=_rc_df["revenue"], y=_rc_df["consultant"], orientation="h", name="Behaald",
    marker_color=_rc_df["color"], marker_line_width=0,
    text=_rc_df.apply(lambda r: f"€{r['revenue']:,.0f}  ({r['pct']:.0f}%)", axis=1),
    textposition="inside", insidetextanchor="middle", textfont=dict(color="white",size=12),
    customdata=_rc_df[["target","pct"]].values,
    hovertemplate="<b>%{y}</b><br>Behaald: €%{x:,.0f}<br>Target: €%{customdata[0]:,.0f}<br>%{customdata[1]:.0f}%<extra></extra>"))
_fig3.add_trace(go.Bar(x=_rc_df["resterend"], y=_rc_df["consultant"], orientation="h", name="Resterend",
    marker_color="rgba(255,255,255,0.05)", marker_line_color="rgba(255,255,255,0.1)", marker_line_width=1,
    hovertemplate="<b>%{y}</b><br>Nog te gaan: €%{x:,.0f}<extra></extra>"))
_fig3.update_layout(barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
    font_color="rgba(255,255,255,0.5)",
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickprefix="€", tickfont=dict(size=10), zeroline=False),
    yaxis=dict(tickfont=dict(size=13), gridcolor="rgba(0,0,0,0)"),
    legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
        font=dict(color="rgba(255,255,255,0.35)", size=10)),
    margin=dict(l=0,r=10,t=30,b=0), height=max(300, len(_rc_df)*42), bargap=0.22, autosize=True)
_bar_h = ch(_fig3)

screen0_html = (
    '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-bottom:0.8rem">'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Dagen resterend</div>'
    + f'<div class="card-val" style="color:{dc}">{days_rem}</div>'
    + f'<div class="card-sub">van {tot_days} dagen in {CURRENT_MONTH}</div></div>'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Maandomzet</div>'
    + f'<div class="card-val pink">€{tot:,.0f}</div>'
    + f'<div class="card-sub">{pct:.0f}% van €{COMPANY_TARGET:,.0f} target</div>'
    + f'<div class="prog"><div class="prog-fill" style="width:{pct:.1f}%"></div></div></div>'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Open Vacatures</div>'
    + f'<div class="card-val teal">{open_vac_count}</div><div class="card-sub">actief uitstaand</div></div>'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Omzetverwachting</div>'
    + f'<div class="card-val" style="color:{fc};font-size:2.2rem">{ft}</div>'
    + f'<div class="card-sub">{fs}</div></div>'
    + '</div>'
    + '<div style="display:grid;grid-template-columns:1fr 2.2fr;gap:1rem;margin-bottom:0.5rem">'
    + f'<div>{_donut_h}</div><div>{_right_h}</div></div>'
    + f'<div>{_bar_h}</div>'
)

# ══════════════════════════════════════════════════════
# BUILD SCREEN 1 — PIPELINE
# ══════════════════════════════════════════════════════
_p   = pipeline
_ttf = f"{_p['avg_ttf']} dgn" if _p['avg_ttf'] else "—"
_tth = f"{_p['avg_tth']} dgn" if _p['avg_tth'] else "—"
_q_start = (nl_now()-timedelta(days=90)).strftime("%-d %B")
_q_end   = nl_now().strftime("%-d %B %Y")
for _en,_nl in {"January":"januari","February":"februari","March":"maart","April":"april",
                "May":"mei","June":"juni","July":"juli","August":"augustus",
                "September":"september","October":"oktober","November":"november","December":"december"}.items():
    _q_start=_q_start.replace(_en,_nl); _q_end=_q_end.replace(_en,_nl)

_instroom = _p["op_gesprek"] + _p["eerste_gesprek"] + _p["aanbod"] + _p["geplaatst"]
_kpis = [("Instroom",_instroom,"#f5a623"),("Op Gesprek",_p["op_gesprek"],"#00d4c8"),
         ("Gesprek Bij Klant",_p["eerste_gesprek"],"#e92076"),
         ("Heeft Aanbod",_p["aanbod"],"#a78bfa"),("Geplaatst ✓",_p["geplaatst"],"#00e5a0")]
_kpi_html = "".join(
    f'<div class="fkpi"><div class="fkpi-val" style="color:{_c}">{_v}</div>'
    f'<div class="fkpi-lbl">{_l}</div></div>'
    for _l,_v,_c in _kpis)

_fig_f = go.Figure(go.Funnel(
    y=["Instroom","Op Gesprek","Gesprek Bij Klant","Aanbod","Geplaatst"],
    x=[_instroom,_p["op_gesprek"],_p["eerste_gesprek"],_p["aanbod"],_p["geplaatst"]],
    textinfo="value+percent initial",
    marker=dict(color=["#f5a623","#00d4c8","#e92076","#a78bfa","#00e5a0"],line=dict(width=0)),
    connector=dict(line=dict(color="rgba(255,255,255,0.05)",width=2)),
    textfont=dict(size=14,color="white",family="Inter")))
_fig_f.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
    font_color="rgba(255,255,255,0.5)", margin=dict(l=160,r=40,t=10,b=10),
    height=300, autosize=True, yaxis=dict(tickfont=dict(size=12,family="Inter")))
_funnel_h = ch(_fig_f)

_arrow = "↑" if delta>=0 else "↓"
_clr2  = "#00e5a0" if delta>=0 else "#e92076"

screen1_html = (
    f'<div style="font-size:0.58rem;color:rgba(255,255,255,0.2);letter-spacing:3px;'
    f'text-transform:uppercase;margin-bottom:1rem;">Kwartaal · {_q_start} – {_q_end}</div>'
    + '<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:0.8rem;margin-bottom:1rem">'
    + _kpi_html + '</div>'
    + '<div style="display:grid;grid-template-columns:1.4fr 1fr;gap:1rem;margin-bottom:0.5rem">'
    + f'<div>{_funnel_h}</div>'
    + '<div>'
    + f'<div class="card" style="margin-bottom:0.8rem"><div class="card-top"></div>'
    + f'<div class="card-label">vs vorige maand (t/m dag {today_day})</div>'
    + f'<div style="font-family:Syne,sans-serif;font-size:1.8rem;font-weight:800;'
    + f'color:{_clr2};margin-top:0.2rem">{_arrow} €{abs(delta):,.0f}</div>'
    + f'<div class="card-sub">{dpc:+.1f}%</div></div>'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.8rem">'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Gem. Time to Fill</div>'
    + f'<div class="card-val teal" style="font-size:2rem">{_ttf}</div></div>'
    + f'<div class="card"><div class="card-top"></div><div class="card-label">Gem. Time to Hire</div>'
    + f'<div class="card-val pink" style="font-size:2rem">{_tth}</div></div>'
    + '</div></div></div>'
    + f'<div>{_bar_h}</div>'
)

# ══════════════════════════════════════════════════════
# BUILD SCREEN 2 — VACATURES
# ══════════════════════════════════════════════════════
_vac_cards = ""
_sidebar   = ""
_num_vacs  = len(vacs)

for _i, _v in enumerate(vacs):
    _title   = _v.get("jobTitle","—")
    _company = (_v.get("toCompany") or {}).get("name","—")
    _ow      = _v.get("owner") or {}
    _cons    = f"{_ow.get('firstName','')} {_ow.get('lastName','')}".strip() or "—"
    _created = pd.to_datetime(_v.get("creationDate")) if _v.get("creationDate") else None
    _dopen   = (nl_now()-_created.replace(tzinfo=None)).days if _created else 0
    _loc     = _v.get("workLocation","") or ""
    if _loc in ("None","nan"): _loc=""
    _vno     = _v.get("vacancyNo","")
    _agency  = _v.get("agency","") or ""
    if _agency in ("None","nan"): _agency=""
    _intro   = smart_truncate(_v.get("intro","") or "")
    _dc      = "#e92076" if _dopen>60 else "#f5a623" if _dopen>30 else "#00d4c8"
    _pills   = (f'<span class="pill">📍 {_loc}</span>' if _loc else "")
    _disp    = "block" if _i==0 else "none"
    _agency_span = f"<span style='font-size:0.72rem;color:rgba(255,255,255,0.3);'>· {_agency}</span>" if _agency else ""
    _intro_html  = (f"<div style='font-size:1rem;color:rgba(255,255,255,0.55);line-height:1.7;'>{_intro}</div>") if _intro else ""
    _vac_cards += (
        f'<div class="vac-card" id="vc-{_i}" style="display:{_disp}">'
        f'<div class="vac-big">'
        f'<div class="vac-num">{_i+1} van {_num_vacs} · vacature {_vno}</div>'
        f'<div class="vac-title">{_title}</div>'
        f'<div class="vac-company">🏢 {_company}</div>'
        f'<div class="pills">{_pills}</div>'
        f'<div style="margin:0.8rem 0 1rem;padding:1rem 1.1rem;background:rgba(42,8,69,0.5);'
        f'border-radius:10px;border-left:2px solid rgba(0,212,200,0.35);">'
        f'<div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:0.6rem;flex-wrap:wrap;">'
        f'<span style="font-size:0.95rem;font-weight:600;color:white;">👤 {_cons}</span>{_agency_span}</div>'
        f'<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.7rem;">'
        f'<span style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);'
        f'border-radius:20px;padding:0.2rem 0.7rem;font-size:0.7rem;color:rgba(255,255,255,0.45);">#{_vno}</span>'
        f'<span style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);'
        f'border-radius:20px;padding:0.2rem 0.7rem;font-size:0.7rem;color:{_dc};">⏱ {_dopen} dagen open</span>'
        f'</div>{_intro_html}</div>'
        f'</div></div>'
    )
    # Sidebar
    _cr2 = pd.to_datetime(_v.get("creationDate")) if _v.get("creationDate") else None
    _do2 = (nl_now()-_cr2.replace(tzinfo=None)).days if _cr2 else 0
    _dc2 = "#e92076" if _do2>60 else "#f5a623" if _do2>30 else "rgba(255,255,255,0.2)"
    _r2  = _cons
    _cls2= "vac-sm cur" if _i==0 else "vac-sm"
    _sidebar += (
        f'<div class="{_cls2}" id="vsm-{_i}">'
        f'<div class="vac-sm-t">{_title}</div>'
        f'<div class="vac-sm-c" style="margin-top:0.15rem;">'
        f'<span style="color:rgba(255,255,255,0.45);font-weight:500">{_r2}</span>'
        f' · {_company} · <span style="color:{_dc2}">{_do2}d</span></div></div>'
    )

_dots = "".join(f'<div class="dot{"" if _i else " on"}" id="dot-{_i}"></div>' for _i in range(_num_vacs))

_btn_style = ("background:rgba(42,8,69,0.7);border:1px solid rgba(233,32,118,0.3);"
              "color:rgba(255,255,255,0.6);border-radius:6px;padding:0.3rem 0.9rem;"
              "font-size:0.72rem;cursor:pointer;font-family:Inter,sans-serif;margin-right:0.4rem;")

screen2_html = (
    '<div style="margin-bottom:0.8rem">'
    f'<a href="https://www.lincks.nl/vacatures" target="_blank" style="text-decoration:none;">'
    f'<div style="background:linear-gradient(135deg,rgba(233,32,118,0.15),rgba(0,212,200,0.08));'
    f'border:1px solid rgba(233,32,118,0.3);border-radius:14px;padding:0.9rem 1.4rem;'
    f'display:inline-flex;align-items:center;gap:1rem;">'
    f'<div style="font-family:Syne,sans-serif;font-size:2.6rem;font-weight:800;color:#e92076;line-height:1;">{open_vac_count}</div>'
    f'<div><div style="font-size:0.58rem;color:rgba(255,255,255,0.3);letter-spacing:3px;text-transform:uppercase;">open vacatures</div>'
    f'<div style="font-size:0.7rem;color:rgba(255,255,255,0.2);margin-top:0.2rem;">lincks.nl/vacatures ↗</div></div>'
    f'</div></a></div>'
    + '<div style="display:grid;grid-template-columns:2.5fr 1fr;gap:1.2rem">'
    + f'<div>{_vac_cards}'
    + f'<div class="dots" id="vac-dots">{_dots}</div>'
    + f'<div style="margin-top:0.5rem">'
    + f'<button onclick="prevVac()" style="{_btn_style}">← Vorige</button>'
    + f'<button onclick="nextVac()" style="{_btn_style}">Volgende →</button>'
    + '</div></div>'
    + '<div><div style="font-size:0.52rem;color:rgba(255,255,255,0.18);letter-spacing:3px;'
    + 'text-transform:uppercase;margin-bottom:0.6rem;padding-left:0.3rem">Per recruiter</div>'
    + f'{_sidebar}</div></div>'
) if _num_vacs else '<div style="color:rgba(255,255,255,0.4);padding:2rem">Geen actieve vacatures.</div>'

# ══════════════════════════════════════════════════════
# ASSEMBLE & RENDER
# ══════════════════════════════════════════════════════
_nav_btn = ("background:rgba(42,8,69,0.6);border:1px solid rgba(233,32,118,0.2);"
            "color:rgba(255,255,255,0.4);border-radius:6px;font-size:0.62rem;"
            "letter-spacing:2px;text-transform:uppercase;font-weight:500;"
            "padding:0.3rem 0.75rem;cursor:pointer;font-family:Inter,sans-serif;"
            "transition:all 0.15s;margin-right:0.4rem;")

_js = """
<script>
var D=['Zondag','Maandag','Dinsdag','Woensdag','Donderdag','Vrijdag','Zaterdag'];
var M=['januari','februari','maart','april','mei','juni','juli','augustus',
       'september','oktober','november','december'];
function getNL(){return new Date(new Date().toLocaleString("en-US",{timeZone:"Europe/Amsterdam"}));}
function tickClock(){
  var n=getNL();
  var ce=document.getElementById('tv-time');var de=document.getElementById('tv-date');
  if(ce)ce.textContent=('0'+n.getHours()).slice(-2)+':'+('0'+n.getMinutes()).slice(-2)+':'+('0'+n.getSeconds()).slice(-2);
  if(de)de.textContent=(D[n.getDay()]+' '+n.getDate()+' '+M[n.getMonth()]+' '+n.getFullYear()).toUpperCase();
}
tickClock();setInterval(tickClock,1000);

var curScr=0,lastSw=Date.now(),vacIdx=0,vacLastSw=Date.now();
""" + f"var numVacs={_num_vacs};" + """
function switchScreen(n,manual){
  document.querySelectorAll('.tv-screen').forEach(function(el,i){el.style.display=i===n?'':'none';});
  document.querySelectorAll('.tv-nav-btn').forEach(function(el,i){
    el.style.background=i===n?'rgba(233,32,118,0.2)':'rgba(42,8,69,0.6)';
    el.style.color=i===n?'white':'rgba(255,255,255,0.4)';
    el.style.borderColor=i===n?'rgba(233,32,118,0.5)':'rgba(233,32,118,0.2)';
  });
  curScr=n;if(manual)lastSw=Date.now();
}
function showVac(i){
  if(numVacs===0)return;
  document.querySelectorAll('.vac-card').forEach(function(el,j){el.style.display=j===i?'block':'none';});
  document.querySelectorAll('[id^="dot-"]').forEach(function(el,j){el.className='dot'+(j===i?' on':'');});
  document.querySelectorAll('[id^="vsm-"]').forEach(function(el,j){el.className=j===i?'vac-sm cur':'vac-sm';});
  vacIdx=i;
}
function nextVac(){showVac((vacIdx+1)%numVacs);vacLastSw=Date.now();}
function prevVac(){showVac((vacIdx-1+numVacs)%numVacs);vacLastSw=Date.now();}
setInterval(function(){if(Date.now()-lastSw>=60000){switchScreen((curScr+1)%3,false);lastSw=Date.now();}},1000);
setInterval(function(){if(curScr===2&&numVacs>0&&Date.now()-vacLastSw>=4000){showVac((vacIdx+1)%numVacs);vacLastSw=Date.now();}},1000);
switchScreen(0,false);
</script>
"""

_iframe_css = """
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=Inter:wght@300;400;500;600;700&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
html,body{font-family:'Inter',sans-serif;background:#180329;color:white;overflow:hidden;}
body::before{content:'';position:fixed;top:0;left:0;right:0;bottom:0;
  background-image:linear-gradient(rgba(233,32,118,0.04) 1px,transparent 1px),
  linear-gradient(90deg,rgba(233,32,118,0.04) 1px,transparent 1px);
  background-size:60px 60px;pointer-events:none;z-index:0;}
.wrap{position:relative;z-index:1;padding:0.9rem 2.5rem 1rem;}
.hdr{display:flex;align-items:center;margin-bottom:0.7rem;}
.hdr-left{display:flex;align-items:center;gap:1.2rem;}
.hdr-badge{background:#e92076;color:white;font-size:0.55rem;font-weight:700;
  letter-spacing:3px;padding:0.25rem 0.7rem;border-radius:3px;text-transform:uppercase;}
.divider{height:1px;background:rgba(233,32,118,0.15);margin-bottom:0.6rem;}
.card{background:linear-gradient(135deg,#2a0845,#1e0535);border:1px solid rgba(233,32,118,0.3);
  border-radius:14px;padding:1.5rem;position:relative;overflow:hidden;}
.card-top{position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#e92076,#63ccca);}
.card-label{font-size:0.58rem;font-weight:600;color:rgba(255,255,255,0.5);text-transform:uppercase;letter-spacing:3px;margin-bottom:0.7rem;}
.card-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.pink{color:#e92076;}.teal{color:#00d4c8;}.green{color:#00e5a0;}.orange{color:#f5a623;}
.card-sub{font-size:0.7rem;color:rgba(255,255,255,0.35);margin-top:0.5rem;}
.prog{height:3px;background:rgba(255,255,255,0.08);border-radius:2px;margin-top:1rem;overflow:hidden;}
.prog-fill{height:100%;background:linear-gradient(90deg,#e92076,#63ccca);border-radius:2px;}
.fkpi{background:linear-gradient(135deg,#2a0845,#1e0535);border:1px solid rgba(233,32,118,0.3);
  border-radius:12px;padding:1.5rem 1rem;text-align:center;}
.fkpi-val{font-family:'Syne',sans-serif;font-size:3rem;font-weight:800;line-height:1;}
.fkpi-lbl{font-size:0.58rem;color:rgba(255,255,255,0.5);letter-spacing:2px;text-transform:uppercase;margin-top:0.5rem;}
.vac-big{background:linear-gradient(145deg,#1e0535 0%,#2a0845 60%,#180329 100%);
  border:1px solid rgba(233,32,118,0.3);border-radius:18px;padding:2.2rem 2.5rem;
  position:relative;overflow:hidden;min-height:340px;animation:slidein 0.4s cubic-bezier(0.4,0,0.2,1);}
@keyframes slidein{from{opacity:0;transform:translateX(40px);}to{opacity:1;transform:translateX(0);}}
.vac-big::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,#e92076 0%,#63ccca 50%,transparent 100%);}
.vac-big::after{content:'';position:absolute;top:-80px;right:-80px;width:300px;height:300px;
  background:radial-gradient(circle,rgba(233,32,118,0.06) 0%,transparent 70%);pointer-events:none;}
.vac-num{font-size:0.58rem;color:rgba(255,255,255,0.2);letter-spacing:3px;text-transform:uppercase;margin-bottom:0.7rem;}
.vac-title{font-family:'Syne',sans-serif;font-size:2.4rem;font-weight:800;color:white;line-height:1.1;margin-bottom:0.6rem;}
.vac-company{font-size:1rem;color:#00d4c8;font-weight:600;margin-bottom:0.5rem;}
.pills{display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem;}
.pill{background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);
  border-radius:20px;padding:0.3rem 0.85rem;font-size:0.72rem;color:rgba(255,255,255,0.55);}
.pill.acc{background:rgba(0,212,200,0.1);border-color:rgba(0,212,200,0.25);color:#00d4c8;}
.dots{display:flex;gap:0.35rem;justify-content:center;margin-top:1rem;}
.dot{width:5px;height:5px;border-radius:50%;background:rgba(255,255,255,0.1);}
.dot.on{background:#e92076;width:18px;border-radius:3px;}
.vac-sm{background:rgba(42,8,69,0.5);border:1px solid rgba(233,32,118,0.15);
  border-left:2px solid rgba(233,32,118,0.4);border-radius:8px;padding:0.65rem 0.9rem;margin-bottom:0.4rem;}
.vac-sm.cur{border-left-color:#63ccca;background:rgba(99,204,202,0.05);}
.vac-sm-t{font-size:0.8rem;font-weight:600;color:white;margin-bottom:0.1rem;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.vac-sm-c{font-size:0.65rem;color:rgba(255,255,255,0.4);}
.nav-tabs{display:flex;gap:0.4rem;margin-bottom:1.4rem;align-items:center;}
"""

_body = (
    '<div class="wrap">'
    + f'<div class="hdr" style="display:flex;align-items:center;justify-content:space-between;">'
    + f'<div class="hdr-left">{logo_html}<span class="hdr-badge">Live · {CURRENT_MONTH}</span></div>'
    + '<div style="text-align:right;">'
    + '<div id="tv-time" style="font-family:\'Syne\',\'Arial Black\',sans-serif;font-size:2.6rem;font-weight:800;'
    + 'color:#e92076;line-height:1;letter-spacing:-1px;text-shadow:0 0 30px rgba(233,32,118,0.4);">--:--:--</div>'
    + '<div id="tv-date" style="font-family:Inter,sans-serif;font-size:0.58rem;'
    + 'color:rgba(255,255,255,0.22);letter-spacing:3px;text-transform:uppercase;margin-top:3px;">--</div>'
    + '</div></div>'
    + '<div class="divider" style="margin-top:0.5rem"></div>'
    + '<div class="nav-tabs">'
    + f'<button class="tv-nav-btn" onclick="switchScreen(0,true)" style="{_nav_btn}">Omzet</button>'
    + f'<button class="tv-nav-btn" onclick="switchScreen(1,true)" style="{_nav_btn}">Pipeline</button>'
    + f'<button class="tv-nav-btn" onclick="switchScreen(2,true)" style="{_nav_btn}">★ Vacatures</button>'
    + '</div>'
    + f'<div id="scr-0" class="tv-screen">{screen0_html}</div>'
    + f'<div id="scr-1" class="tv-screen" style="display:none">{screen1_html}</div>'
    + f'<div id="scr-2" class="tv-screen" style="display:none">{screen2_html}</div>'
    + '<div style="text-align:center;padding:0.8rem 0 0.3rem;color:rgba(255,255,255,0.05);'
    + 'font-size:0.52rem;letter-spacing:3px;text-transform:uppercase">'
    + 'Data gecached · vernieuwd elke 5 minuten</div>'
    + '</div>'
)

components.html(
    f'<!DOCTYPE html><html><head><meta charset="utf-8">'
    f'<style>{_iframe_css}</style>'
    f'</head><body>'
    f'{_body}'
    f'{_js}'
    f'</body></html>',
    height=1050,
    scrolling=False
)