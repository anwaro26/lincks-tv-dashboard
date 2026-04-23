import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import time
from datetime import datetime, timedelta
import requests
import base64
import pytz

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Lincks TV Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown('<meta http-equiv="refresh" content="1800">', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# STATE (TV SYSTEM)
# ─────────────────────────────────────────────
if "screen" not in st.session_state:
    st.session_state.screen = 1

if "last_switch" not in st.session_state:
    st.session_state.last_switch = time.time()

if "loaded" not in st.session_state:
    st.session_state.loaded = False

# switch every 60 seconds
if time.time() - st.session_state.last_switch > 60:
    st.session_state.screen += 1
    if st.session_state.screen > 3:
        st.session_state.screen = 1
    st.session_state.last_switch = time.time()
    st.rerun()

# ─────────────────────────────────────────────
# TIME (STABLE)
# ─────────────────────────────────────────────
def get_time():
    tz = pytz.timezone("Europe/Amsterdam")
    now = datetime.now(tz)
    return now.strftime("%H:%M"), now.strftime("%A %d %B %Y").upper()

clock, date = get_time()

# ─────────────────────────────────────────────
# LOADING SCREEN
# ─────────────────────────────────────────────
loader = st.empty()

if not st.session_state.loaded:
    loader.markdown(f"""
    <div style="position:fixed;top:0;left:0;width:100%;height:100%;
        background:#0a0a14;display:flex;flex-direction:column;
        justify-content:center;align-items:center;z-index:9999;">
        <img src="https://www.lincks.nl/uploads/lincks_logo_fc-wit_def.png" style="height:80px;" />
        <div style="color:#e92076;font-size:5rem;font-family:Bebas Neue">{clock}</div>
        <div style="color:rgba(255,255,255,0.5)">{date}</div>
    </div>
    """, unsafe_allow_html=True)

    time.sleep(2)
    st.session_state.loaded = True
    loader.empty()

# ─────────────────────────────────────────────
# STYLING
# ─────────────────────────────────────────────
st.markdown("""
<style>
html, body {
    background-color:#0a0a14;
    font-family:Inter;
}
.stApp { background-color:#0a0a14; }

.tv-header {
    display:flex;
    justify-content:space-between;
    align-items:center;
    border-bottom:2px solid rgba(233,32,118,0.3);
    padding-bottom:1rem;
    margin-bottom:1rem;
}

#clock {
    font-size:2.5rem;
    color:#e92076;
    font-family:Bebas Neue;
}
#date {
    font-size:0.8rem;
    color:rgba(255,255,255,0.4);
    text-transform:uppercase;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown(f"""
<div class="tv-header">
    <div>
        <img src="https://www.lincks.nl/uploads/lincks_logo_fc-wit_def.png" style="height:45px;">
    </div>
    <div style="text-align:right;">
        <div id="clock">{clock}</div>
        <div id="date">{date}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# MOCK DATA (REPLACE WITH YOUR PIPELINE)
# ─────────────────────────────────────────────
df = pd.DataFrame({
    "consultant": ["A", "B", "C"],
    "revenue": [12000, 25000, 18000],
    "target": [20000, 20000, 20000]
})

# fake funnel data
funnel_data = {
    "1e gesprekken bij klant": 120,
    "2e gesprekken bij klant": 80,
    "Heeft aanbod": 50,
    "Plaatsingen": 25
}

vacancies = pd.DataFrame({
    "job_title": ["Dev", "Sales", "Engineer"],
    "company": ["A", "B", "C"],
    "status": ["Open", "Open", "Closed"],
    "match_count": [5, 2, 8]
})

# ─────────────────────────────────────────────
# SCREEN 1 — REVENUE
# ─────────────────────────────────────────────
def screen_revenue():
    st.title("Revenue Dashboard")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["consultant"],
        y=df["revenue"],
        name="Revenue",
        marker_color="#e92076"
    ))

    fig.add_trace(go.Bar(
        x=df["consultant"],
        y=df["target"] - df["revenue"],
        name="Remaining",
        marker_color="rgba(255,255,255,0.1)"
    ))

    fig.update_layout(
        barmode="stack",
        paper_bgcolor="#0a0a14",
        plot_bgcolor="#0a0a14",
        font_color="white"
    )

    st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# SCREEN 2 — FUNNEL
# ─────────────────────────────────────────────
def screen_funnel():
    st.title("Recruitment Funnel")

    fig = go.Figure(go.Funnel(
        y=list(funnel_data.keys()),
        x=list(funnel_data.values())
    ))

    fig.update_layout(
        paper_bgcolor="#0a0a14",
        plot_bgcolor="#0a0a14",
        font_color="white"
    )

    st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────
# SCREEN 3 — VACANCIES
# ─────────────────────────────────────────────
def screen_vacancies():
    st.title("Open Vacatures")

    st.dataframe(vacancies, use_container_width=True)

# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
if st.session_state.screen == 1:
    screen_revenue()

elif st.session_state.screen == 2:
    screen_funnel()

elif st.session_state.screen == 3:
    screen_vacancies()

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;color:rgba(255,255,255,0.2);
font-size:11px;margin-top:20px;">
TV MODE • auto switch 60s
</div>
""", unsafe_allow_html=True)