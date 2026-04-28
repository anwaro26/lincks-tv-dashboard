import streamlit as st
import pandas as pd
import time
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Lincks TV Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    html, body, [class*="css"]  {
        background-color: #0a0a14;
        color: white;
        font-family: Inter, sans-serif;
    }

    .title {
        font-size: 52px;
        font-weight: 800;
        color: #e92076;
        text-align: center;
        margin-top: 10px;
    }

    .kpi {
        font-size: 90px;
        font-weight: 900;
        text-align: center;
        color: #ffffff;
    }

    .sub {
        text-align: center;
        color: rgba(255,255,255,0.5);
        font-size: 18px;
        margin-bottom: 20px;
    }

    .card {
        background: rgba(255,255,255,0.05);
        padding: 30px;
        border-radius: 20px;
        text-align: center;
        margin: 10px;
    }

    .row {
        display: flex;
        justify-content: space-around;
        margin-top: 30px;
    }

</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# INIT STATE
# ─────────────────────────────────────────────
if "screen" not in st.session_state:
    st.session_state.screen = 0
    st.session_state.last_switch = time.time()

# ─────────────────────────────────────────────
# CLOCK
# ─────────────────────────────────────────────
def get_time():
    return datetime.now().strftime("%H:%M:%S")

def get_date():
    return datetime.now().strftime("%A %d %B %Y")

# ─────────────────────────────────────────────
# SCREEN ROTATION (60 sec)
# ─────────────────────────────────────────────
if time.time() - st.session_state.last_switch > 60:
    st.session_state.screen = (st.session_state.screen + 1) % 3
    st.session_state.last_switch = time.time()
    st.rerun()

screen = st.session_state.screen

# ─────────────────────────────────────────────
# MOCK DATA (vervang later met je API)
# ─────────────────────────────────────────────
revenue = 248000
target = 250000
pct = int((revenue / target) * 100)

funnel = {
    "1e gesprek": 42,
    "2e gesprek": 27,
    "Aanbod": 18,
    "Plaatsing": 11
}

vacancies = [
    "Accountmanager Amsterdam",
    "Recruiter Rotterdam",
    "Consultant Utrecht",
    "Sales Lead Eindhoven"
]

# ─────────────────────────────────────────────
# SCREEN 1 — KPI
# ─────────────────────────────────────────────
if screen == 0:
    st.markdown('<div class="title">LINCKS PERFORMANCE</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub">{get_date()} • {get_time()}</div>', unsafe_allow_html=True)

    st.markdown(f'<div class="kpi">€{revenue:,.0f}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub">Maandomzet</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="row">
        <div class="card">
            <h2>{pct}%</h2>
            <p>Target bereikt</p>
        </div>
        <div class="card">
            <h2>€{target:,.0f}</h2>
            <p>Doelstelling</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SCREEN 2 — FUNNEL
# ─────────────────────────────────────────────
elif screen == 1:
    st.markdown('<div class="title">CANDIDATE FUNNEL</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub">{get_time()}</div>', unsafe_allow_html=True)

    cols = st.columns(4)

    for i, (k, v) in enumerate(funnel.items()):
        with cols[i]:
            st.markdown(f"""
            <div class="card">
                <div style="font-size:40px;font-weight:900;color:#e92076">{v}</div>
                <div style="margin-top:10px;color:rgba(255,255,255,0.7)">{k}</div>
            </div>
            """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SCREEN 3 — VACATURES
# ─────────────────────────────────────────────
elif screen == 2:
    st.markdown('<div class="title">OPEN VACATURES</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="sub">{get_time()}</div>', unsafe_allow_html=True)

    for v in vacancies:
        st.markdown(f"""
        <div class="card" style="text-align:left">
            <span style="color:#e92076">●</span> {v}
        </div>
        """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# FOOTER TIMER INFO
# ─────────────────────────────────────────────
st.markdown(f"""
<div style="position:fixed;bottom:10px;right:20px;color:rgba(255,255,255,0.3);font-size:12px">
Screen {screen+1}/3 • switch elke 60 sec
</div>
""", unsafe_allow_html=True)