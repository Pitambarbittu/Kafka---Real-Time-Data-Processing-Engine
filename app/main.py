# import streamlit as st
# import pandas as pd
# import numpy as np
# import time
# from sklearn.ensemble import IsolationForest

# # --- PROJECT IDENTITY ---
# st.set_page_config(page_title="NIT Srinagar | Real-Time Engine", layout="wide")

# st.title("🚀 Intelligent Real-Time Data Processing Engine")
# st.markdown(f"""
# **Candidate:** Alina Pervaiz | **Enrolment:** 2024MCSECS019  
# **Supervisors:** Dr. Shaima Qureshi & Dr. Mahreen Salim
# """)

# # --- SYSTEM STATE INITIALIZATION ---
# if 'processed_count' not in st.session_state:
#     st.session_state.processed_count = 0
# if 'buffer' not in st.session_state:
#     st.session_state.buffer = pd.DataFrame(columns=['Timestamp', 'Workload', 'State', 'Window_ID'])

# # --- THE PROCESSING ENGINE ---
# def run_realtime_pipeline():
#     # 1. SIMULATE INGESTION (Hadoop Replacement Logic)
#     # We process in 'Windows' as per your flowchart
#     window_id = f"W-{st.session_state.processed_count // 10}"
#     new_data = np.random.normal(60, 10)
    
#     # Inject high-velocity spikes (Anomalies)
#     if np.random.random() > 0.95: 
#         new_data += 65 

#     # 2. ML: ISOLATION FOREST (Anomaly Detection)
#     # This is the 'Intelligent Management' part of your PPT
#     is_anomaly = False
#     if len(st.session_state.buffer) > 10:
#         model = IsolationForest(contamination=0.1)
#         preds = model.fit_predict(st.session_state.buffer[['Workload']])
#         is_anomaly = True if preds[-1] == -1 else False

#     # 3. UPDATE SYSTEM METRICS
#     state = "CRITICAL" if is_anomaly else "STABLE"
#     new_msg = pd.DataFrame({
#         'Timestamp': [pd.Timestamp.now()],
#         'Workload': [new_data],
#         'State': [state],
#         'Window_ID': [window_id]
#     })
    
#     st.session_state.buffer = pd.concat([st.session_state.buffer, new_msg]).tail(50)
#     st.session_state.processed_count += 1

# # --- LIVE DASHBOARD UI ---
# # Row 1: High-Level Stats
# m1, m2, m3, m4 = st.columns(4)
# run_realtime_pipeline()

# latest_data = st.session_state.buffer.iloc[-1]

# m1.metric("Total Data Packets", st.session_state.processed_count)
# m2.metric("Active Window", latest_data['Window_ID'])
# m3.metric("Current Workload", f"{latest_data['Workload']:.2f}")
# m4.metric("System Health", latest_data['State'], delta_color="inverse")

# # Row 2: Visual Charts
# st.divider()
# c1, c2 = st.columns([2, 1])

# with c1:
#     st.subheader("📡 Real-Time Kafka Stream")
#     # Show the line chart of the workload
#     st.line_chart(st.session_state.buffer.set_index('Timestamp')['Workload'])

# with c2:
#     st.subheader("🛠️ Auto-Scaling Logic")
#     # Logic based on your PPT Slide 15: Dynamically scaling partitions
#     if latest_data['Workload'] > 90:
#         st.warning("⚠️ High Load Detected")
#         st.progress(90, text="Scaling Partitions: 3 -> 6")
#     elif latest_data['State'] == "CRITICAL":
#         st.error("🚨 Anomaly: Rerouting Traffic")
#     else:
#         st.success("✅ Load Optimal")
#         st.progress(30, text="Partitions: 3 (Stable)")

# # Row 3: Raw Window Logs (To verify the 'Window Shifting' you asked for)
# st.subheader("📋 Active Process Window Logs")
# st.dataframe(st.session_state.buffer.sort_values(by='Timestamp', ascending=False), use_container_width=True)

# # Loop the app
# time.sleep(0.5)
# st.rerun()


"""
=============================================================
  Real-Time Data Processing Engine using Apache Kafka
  Candidate  : Alina Pervaiz | 2024MCSECS019
  Institute  : NIT Srinagar — CSE Department | MTech 3rd Sem
  Supervisors: Dr. Shaima Qureshi & Dr. Mahreen Salim
  
  FIX v2: Zero-flicker refresh + responsive Critical Apps layout
=============================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import time
import psutil
import platform
from datetime import datetime
from sklearn.ensemble import IsolationForest

st.set_page_config(
    page_title="Kafka Engine | NIT Srinagar",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ──────────────────────────────────────────────
# CSS  — includes flicker-kill + responsive grid fix
# ──────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

/* ===== FLICKER KILL ===== */
/* Remove ALL opacity/transform transitions on stVerticalBlock children */
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlock"] > div,
[data-testid="element-container"],
[data-testid="stMarkdownContainer"],
.stMarkdown,
.element-container {
    animation: none !important;
    transition: none !important;
    opacity: 1 !important;
    transform: none !important;
}
/* Streamlit's internal fade-in on rerun */
@keyframes fadeIn { from { opacity: 1; } to { opacity: 1; } }
.st-emotion-cache-fade-in,
[class*="fade"],
[class*="Fade"] {
    animation: none !important;
    opacity: 1 !important;
}
/* Kill the global app skeleton flash */
.stApp > header { transition: none !important; }
.stApp { animation: none !important; }

/* ===== BASE THEME ===== */
html, body, [class*="css"], .stApp {
    font-family: 'Inter', sans-serif !important;
    background-color: #0D1117 !important;
    color: #E6EDF3 !important;
}
.main .block-container { padding-top: 20px !important; }

/* ===== SIDEBAR ===== */
[data-testid="stSidebar"] {
    background-color: #161B22 !important;
    border-right: 1px solid #30363D !important;
}
[data-testid="stSidebar"] * { color: #E6EDF3 !important; }
[data-testid="stSidebar"] hr { border-color: #30363D !important; }
[data-testid="stSidebar"] .stSlider > div > div > div { background: #238636 !important; }

/* ===== BUTTONS ===== */
.stButton > button {
    background: #21262D !important; color: #E6EDF3 !important;
    border: 1px solid #30363D !important; border-radius: 8px !important;
    font-weight: 600 !important; font-size: 13px !important;
    padding: 8px 16px !important; transition: background 0.15s, border-color 0.15s !important;
}
.stButton > button:hover { background: #30363D !important; border-color: #58A6FF !important; }
.stDownloadButton > button {
    background: #238636 !important; color: #fff !important;
    border: 1px solid #2EA043 !important; border-radius: 8px !important;
    font-weight: 600 !important; font-size: 12px !important;
    width: 100% !important; padding: 7px 0 !important; margin-bottom: 4px !important;
}
.stDownloadButton > button:hover { background: #2EA043 !important; }

/* ===== TABS ===== */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px; background: #161B22; padding: 4px;
    border-radius: 10px; border: 1px solid #30363D;
    flex-wrap: wrap;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 7px !important; font-weight: 500 !important;
    font-size: 13px !important; color: #8B949E !important;
    border: none !important; background: transparent !important; padding: 8px 16px !important;
    white-space: nowrap;
}
.stTabs [aria-selected="true"] {
    background: #21262D !important; color: #E6EDF3 !important; border: 1px solid #30363D !important;
}

[data-testid="metric-container"] { display: none !important; }

/* ===== KPI CARDS ===== */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 12px; margin-bottom: 20px;
}
.kpi-card {
    background: #161B22; border: 1px solid #30363D; border-radius: 12px;
    padding: 18px 20px; position: relative; overflow: hidden; min-width: 0;
}
.kpi-card:hover { border-color: #58A6FF; }
.kpi-label { font-size: 11px; font-weight: 500; letter-spacing: 0.8px; text-transform: uppercase; color: #8B949E; margin-bottom: 6px; }
.kpi-value { font-family: 'JetBrains Mono', monospace; font-size: 28px; font-weight: 700; line-height: 1; margin-bottom: 6px; }
.kpi-sub { font-size: 11px; color: #8B949E; }
.kpi-badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 10px; font-weight: 600; margin-top: 4px; }
.badge-green { background: rgba(35,134,54,0.25); color: #3FB950; border: 1px solid rgba(63,185,80,0.3); }
.badge-red   { background: rgba(248,81,73,0.15);  color: #F85149; border: 1px solid rgba(248,81,73,0.3); }
.badge-blue  { background: rgba(88,166,255,0.15); color: #58A6FF; border: 1px solid rgba(88,166,255,0.3); }
.badge-yellow{ background: rgba(210,153,34,0.2);  color: #D29922; border: 1px solid rgba(210,153,34,0.3); }

/* ===== STATUS DOTS ===== */
.dot-pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 5px; vertical-align: middle; }
.dot-green-anim { background: #3FB950; box-shadow: 0 0 6px #3FB950; animation: dot-pulse-anim 2s infinite; }
.dot-red-anim   { background: #F85149; box-shadow: 0 0 6px #F85149; animation: dot-pulse-anim 1s infinite; }
@keyframes dot-pulse-anim { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* ===== SHARED COMPONENTS ===== */
.sec-title {
    font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;
    color: #8B949E; margin: 0 0 14px 0; padding-bottom: 8px; border-bottom: 1px solid #21262D;
}
.panel { background: #161B22; border: 1px solid #30363D; border-radius: 12px; padding: 18px; height: 100%; }
.alert-green { background: rgba(35,134,54,0.15); border: 1px solid rgba(63,185,80,0.3);
               color: #3FB950; border-radius: 8px; padding: 10px 14px; font-size: 13px; font-weight: 600; }
.alert-red   { background: rgba(248,81,73,0.12); border: 1px solid rgba(248,81,73,0.3);
               color: #F85149; border-radius: 8px; padding: 10px 14px; font-size: 13px; font-weight: 600; }
.alert-yellow{ background: rgba(210,153,34,0.15); border: 1px solid rgba(210,153,34,0.3);
               color: #D29922; border-radius: 8px; padding: 10px 14px; font-size: 13px; font-weight: 600; }

/* ===== PROGRESS BARS ===== */
.prog-wrap { margin: 10px 0; }
.prog-row  { display: flex; justify-content: space-between; font-size: 11.5px; color: #8B949E; margin-bottom: 4px; }
.prog-row b { color: #E6EDF3; }
.prog-track { height: 7px; background: #21262D; border-radius: 4px; overflow: hidden; }
.prog-fill  { height: 100%; border-radius: 4px; }

/* ===== LOG ROWS ===== */
.log-row {
    display: flex; align-items: center; gap: 10px; padding: 7px 12px; border-radius: 7px;
    font-family: 'JetBrains Mono', monospace; font-size: 11.5px; margin-bottom: 3px;
    background: #161B22; border-left: 3px solid #30363D; flex-wrap: wrap;
}
.log-row.anom { border-left-color: #F85149; background: rgba(248,81,73,0.05); }
.log-ts  { color: #6E7681; min-width: 70px; }
.log-dev { color: #58A6FF; font-weight: 600; min-width: 85px; }
.log-val { color: #8B949E; min-width: 85px; }
.log-ok  { color: #3FB950; font-weight: 600; }
.log-bad { color: #F85149; font-weight: 600; }

.ev-up   { color: #F85149; padding: 5px 10px; background: rgba(248,81,73,0.08);
           border-left: 3px solid #F85149; border-radius: 6px; margin-bottom: 3px;
           font-family: 'JetBrains Mono', monospace; font-size: 11.5px; }
.ev-down { color: #3FB950; padding: 5px 10px; background: rgba(63,185,80,0.08);
           border-left: 3px solid #3FB950; border-radius: 6px; margin-bottom: 3px;
           font-family: 'JetBrains Mono', monospace; font-size: 11.5px; }

/* ===== DATA TABLES ===== */
.stDataFrame { border-radius: 10px !important; overflow: hidden; }
.stDataFrame th { background: #21262D !important; color: #8B949E !important; font-size: 12px !important; }
.stDataFrame td { background: #161B22 !important; color: #E6EDF3 !important; font-size: 12px !important; }

/* ===== SYSTEM MONITOR ===== */
.sys-grid-4 {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px; margin-bottom: 18px;
}
.sys-kpi {
    background: #161B22; border: 1px solid #30363D; border-radius: 12px;
    padding: 16px 18px; min-width: 0;
}
.sys-proc-row {
    display: flex; align-items: center; gap: 10px;
    padding: 9px 14px; border-radius: 8px; margin-bottom: 5px;
    font-family: 'JetBrains Mono', monospace; font-size: 11.5px;
    background: #0D1117; border-left: 3px solid #30363D; flex-wrap: wrap;
}
.sp-hi { border-left-color: #F85149; background: rgba(248,81,73,0.05); }
.sp-md { border-left-color: #D29922; background: rgba(210,153,34,0.04); }
.sp-lo { border-left-color: #3FB950; background: rgba(63,185,80,0.03); }
.sp-name { color: #58A6FF; font-weight: 600; min-width: 0; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 190px; }
.sp-cpu  { color: #E6EDF3; min-width: 70px; }
.sp-mem  { color: #8B949E; min-width: 70px; }
.sp-pid  { color: #6E7681; font-size: 10px; min-width: 55px; }
.sp-badge { font-size: 10px; padding: 2px 8px; border-radius: 10px; font-weight: 700; white-space: nowrap; }
.sb-hi { background: rgba(248,81,73,0.2); color: #F85149; }
.sb-md { background: rgba(210,153,34,0.2); color: #D29922; }
.sb-lo { background: rgba(63,185,80,0.15); color: #3FB950; }

/* ===== CRITICAL APPS — RESPONSIVE FIX ===== */
/*
   Key fix: use CSS grid with minmax(0,1fr) so columns shrink
   properly when the sidebar is open. Each column has min-width:0
   so content can never force the column wider than its allotted space.
   All text inside uses overflow:hidden + text-overflow:ellipsis.
*/
.crit-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 16px;
    align-items: start;
}

.crit-header {
    background: rgba(248,81,73,0.08); border: 1px solid rgba(248,81,73,0.25);
    border-radius: 10px; padding: 14px 18px; margin-bottom: 16px;
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
}
.crit-col { min-width: 0; }

/* Process row — critical */
.crit-row {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 12px; border-radius: 8px; margin-bottom: 6px;
    background: #0D1117; border: 1px solid rgba(248,81,73,0.2);
    border-left: 4px solid #F85149;
    font-family: 'JetBrains Mono', monospace; font-size: 11.5px;
    min-width: 0; overflow: hidden;
}
/* Process row — warning */
.warn-row {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 12px; border-radius: 8px; margin-bottom: 6px;
    background: #0D1117; border: 1px solid rgba(210,153,34,0.2);
    border-left: 4px solid #D29922;
    font-family: 'JetBrains Mono', monospace; font-size: 11.5px;
    min-width: 0; overflow: hidden;
}

/* Shared cell styles inside crit/warn rows */
.cr-pid  { color: #6E7681; font-size: 10px; white-space: nowrap; flex-shrink: 0; width: 62px; }
.cr-name { font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
.crit-name { color: #F85149; }
.warn-name { color: #D29922; }
.cr-cpu  { color: #E6EDF3; font-weight: 600; white-space: nowrap; flex-shrink: 0; width: 56px; text-align: right; }
.cr-mem  { color: #8B949E; white-space: nowrap; flex-shrink: 0; width: 60px; text-align: right; }
.cr-why  {
    font-size: 10px; padding: 2px 7px; border-radius: 10px; font-weight: 700;
    white-space: nowrap; flex-shrink: 0;
}
.cr-why-crit { background: rgba(248,81,73,0.2); color: #F85149; }
.cr-why-warn { background: rgba(210,153,34,0.2); color: #D29922; }

/* Input widgets */
.stSelectbox > div > div { background: #21262D !important; border: 1px solid #30363D !important; color: #E6EDF3 !important; }
.stCheckbox label { color: #E6EDF3 !important; }
.stTextInput > div > div > input { background: #21262D !important; border: 1px solid #30363D !important; color: #E6EDF3 !important; border-radius: 7px !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# SESSION STATE
# ──────────────────────────────────────────────
def init():
    defaults = {
        'tick': 0, 'running': True,
        'anomaly_total': 0, 'packets_total': 0,
        'partitions': 3, 'scale_events': [],
        'buffer': pd.DataFrame(columns=[
            'timestamp','device','workload','cpu',
            'memory','data_rate','window','state'
        ])
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
init()


# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:10px 0 22px;'>
      <div style='font-size:20px;font-weight:700;color:#E6EDF3;'>⚡ Kafka Engine</div>
      <div style='font-size:11px;color:#6E7681;margin-top:3px;'>NIT Srinagar · MTech CSE 3rd Sem</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="sec-title">🎛️ Engine Controls</div>', unsafe_allow_html=True)
    b1, b2 = st.columns(2)
    with b1:
        if st.button("⏸ Pause" if st.session_state.running else "▶ Start", use_container_width=True):
            st.session_state.running = not st.session_state.running
    with b2:
        if st.button("↺ Reset", use_container_width=True):
            for k in ['tick','anomaly_total','packets_total']:
                st.session_state[k] = 0
            st.session_state.partitions = 3
            st.session_state.scale_events = []
            st.session_state.buffer = pd.DataFrame(columns=[
                'timestamp','device','workload','cpu','memory','data_rate','window','state'
            ])
    st.markdown("---")
    st.markdown('<div class="sec-title">⚙️ Parameters</div>', unsafe_allow_html=True)
    anomaly_rate = st.slider("Anomaly Probability", 0.02, 0.30, 0.08, 0.01)
    refresh_rate = st.slider("Refresh Speed (s)", 0.3, 3.0, 0.7, 0.1)
    window_size  = st.slider("Window Size", 5, 30, 10)
    max_logs     = st.slider("Log Rows Shown", 5, 40, 15)
    chart_pts    = st.slider("Chart History", 20, 150, 60)
    st.markdown("---")
    st.markdown('<div class="sec-title">📥 Download Data</div>', unsafe_allow_html=True)
    buf_dl = st.session_state.buffer
    if len(buf_dl) > 0:
        st.download_button("⬇  Full Stream Data (CSV)",
            data=buf_dl.to_csv(index=False).encode(),
            file_name=f"kafka_stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv", use_container_width=True)
        anom_dl = buf_dl[buf_dl['state'] == 'CRITICAL']
        if len(anom_dl) > 0:
            st.download_button("⬇  Anomalies Only (CSV)",
                data=anom_dl.to_csv(index=False).encode(),
                file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv", use_container_width=True)
        total_dl  = len(buf_dl)
        anom_c_dl = (buf_dl['state'] == 'CRITICAL').sum()
        report = f"""KAFKA ENGINE — SESSION REPORT
Generated  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Candidate  : Alina Pervaiz | 2024MCSECS019
Institute  : NIT Srinagar | MTech CSE 3rd Sem
Supervisors: Dr. Shaima Qureshi & Dr. Mahreen Salim

SUMMARY STATISTICS
Total Packets   : {total_dl}
Anomalies       : {anom_c_dl}
Anomaly Rate    : {(anom_c_dl/total_dl*100) if total_dl>0 else 0:.2f}%
Partitions(final): {st.session_state.partitions}
Avg Workload    : {buf_dl['workload'].mean():.2f}
Avg CPU         : {buf_dl['cpu'].mean():.2f}%
Avg Memory      : {buf_dl['memory'].mean():.1f} MB
Avg Data Rate   : {buf_dl['data_rate'].mean():.2f} MB/s
Peak Workload   : {buf_dl['workload'].max():.2f}

ML MODEL
Algorithm       : Isolation Forest
contamination   : {max(anomaly_rate,0.05):.2f}
n_estimators    : 100
Features        : workload, cpu, memory, data_rate

SCALING LOG
""" + "\n".join(st.session_state.scale_events[-30:])
        st.download_button("⬇  Session Report (TXT)",
            data=report.encode(),
            file_name=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain", use_container_width=True)
    else:
        st.markdown('<div style="font-size:12px;color:#6E7681;">Start the engine to enable downloads.</div>', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown('<div class="sec-title">👤 Project Info</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style='font-size:12px;line-height:2.1;color:#8B949E;'>
      <b style='color:#E6EDF3;'>Alina Pervaiz</b><br>
      Enrolment: 2024MCSECS019<br>
      MTech CSE — 3rd Sem · NIT Srinagar<br><br>
      <b style='color:#C9D1D9;'>Guide:</b> Dr. Shaima Qureshi<br>
      <b style='color:#C9D1D9;'>Co-Supervisor:</b> Dr. Mahreen Salim
    </div>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────
# PIPELINE ENGINE
# ──────────────────────────────────────────────
def run_pipeline():
    st.session_state.tick += 1
    t = st.session_state.tick
    device    = f"sensor_{(t % 5) + 1}"
    is_spike  = np.random.random() < anomaly_rate
    base_cpu  = 25 + 20 * np.sin(t / 25)
    workload  = round(base_cpu + (65 if is_spike else np.random.normal(0, 8)), 2)
    cpu       = round(min(100, max(5, workload + np.random.normal(0, 3))), 2)
    memory    = round(np.random.uniform(750, 950) if is_spike else np.random.uniform(150, 550), 1)
    data_rate = round(np.random.uniform(55, 95)   if is_spike else np.random.uniform(3, 28), 2)
    window_id = f"W-{t // window_size:03d}"
    state     = "STABLE"

    buf = st.session_state.buffer
    if len(buf) >= 15:
        model = IsolationForest(contamination=max(anomaly_rate, 0.05), random_state=42, n_estimators=100)
        model.fit(buf[['workload','cpu','memory','data_rate']].tail(80))
        if model.predict([[workload, cpu, memory, data_rate]])[0] == -1:
            state = "CRITICAL"
            st.session_state.anomaly_total += 1

    old_p = st.session_state.partitions
    if workload > 85 or state == "CRITICAL":
        st.session_state.partitions = min(old_p + 2, 12)
    elif workload < 35 and old_p > 2:
        st.session_state.partitions = max(old_p - 1, 2)
    if st.session_state.partitions != old_p:
        direction = "UP" if st.session_state.partitions > old_p else "DOWN"
        st.session_state.scale_events.append(
            f"[{datetime.now().strftime('%H:%M:%S')}] SCALE {direction}: {old_p} → {st.session_state.partitions}  (WL={workload:.1f})"
        )

    new_row = pd.DataFrame([{
        'timestamp': datetime.now().strftime("%H:%M:%S"),
        'device': device, 'workload': workload, 'cpu': cpu,
        'memory': memory, 'data_rate': data_rate,
        'window': window_id, 'state': state,
    }])
    st.session_state.buffer = pd.concat(
        [st.session_state.buffer, new_row], ignore_index=True
    ).tail(500)
    st.session_state.packets_total += 1

if st.session_state.running:
    run_pipeline()

buf    = st.session_state.buffer
latest = buf.iloc[-1] if len(buf) > 0 else None


# ──────────────────────────────────────────────
# SYSTEM INFO — cached 2s, non-blocking
# ──────────────────────────────────────────────
@st.cache_data(ttl=2)
def get_system_info():
    uname = platform.uname()
    cpu_percent = psutil.cpu_percent(interval=None)
    mem  = psutil.virtual_memory()
    swap = psutil.swap_memory()
    try:
        disk = psutil.disk_usage('/')
        disk_total = disk.total / 1024**3
        disk_used  = disk.used  / 1024**3
        disk_pct   = disk.percent
    except Exception:
        disk_total = disk_used = disk_pct = 0
    try:
        battery = psutil.sensors_battery()
        bat_info = f"{battery.percent:.0f}%" if battery else "N/A"
    except Exception:
        bat_info = "N/A"
    processes = []
    try:
        for proc in psutil.process_iter(['pid','name','cpu_percent','memory_info','status']):
            try:
                pi = proc.info
                mem_mb = (pi['memory_info'].rss / 1048576) if pi['memory_info'] else 0
                processes.append({
                    'PID': pi['pid'],
                    'Application': pi['name'] or '—',
                    'CPU %': round(pi['cpu_percent'] or 0, 1),
                    'Memory (MB)': round(mem_mb, 1),
                    'Status': pi['status'],
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception:
        pass
    processes.sort(key=lambda x: (x['CPU %'], x['Memory (MB)']), reverse=True)
    try:
        net = psutil.net_io_counters()
        net_sent = net.bytes_sent / 1048576
        net_recv = net.bytes_recv / 1048576
    except Exception:
        net_sent = net_recv = 0
    return {
        'os': f"{uname.system} {uname.release}", 'machine': uname.machine, 'node': uname.node,
        'cpu_percent': cpu_percent, 'cpu_count': psutil.cpu_count(),
        'mem_total': mem.total / 1024**3, 'mem_used': mem.used / 1024**3, 'mem_percent': mem.percent,
        'swap_used': swap.used / 1048576, 'swap_percent': swap.percent,
        'disk_total': disk_total, 'disk_used': disk_used, 'disk_percent': disk_pct,
        'battery': bat_info, 'net_sent': net_sent, 'net_recv': net_recv,
        'processes': processes,
    }


# ──────────────────────────────────────────────
# HEADER BAR
# ──────────────────────────────────────────────
is_critical = latest is not None and latest['state'] == 'CRITICAL'
dot_class   = "dot-red-anim" if is_critical else "dot-green-anim"
status_lbl  = "CRITICAL" if is_critical else "RUNNING"

st.markdown(f"""
<div style='display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;
            padding:16px 22px;background:#161B22;border-radius:12px;
            border:1px solid #30363D;margin-bottom:20px;'>
  <div>
    <div style='font-size:20px;font-weight:700;color:#E6EDF3;'>⚡ Real-Time Data Processing Engine</div>
    <div style='font-size:12px;color:#6E7681;margin-top:4px;'>
      Building Data Processing Engine using Apache Kafka &nbsp;·&nbsp; NIT Srinagar CSE Department
    </div>
  </div>
  <div style='display:flex;gap:8px;align-items:center;flex-wrap:wrap;'>
    <span style='background:#21262D;border:1px solid #30363D;border-radius:20px;
                 padding:5px 14px;font-size:12px;font-weight:600;color:#E6EDF3;'>
      <span class="dot-pulse {dot_class}"></span>{status_lbl}
    </span>
    <span style='background:#21262D;border:1px solid #30363D;border-radius:20px;
                 padding:5px 14px;font-size:12px;color:#8B949E;'>
      📦 {st.session_state.packets_total:,} packets
    </span>
    <span style='background:#21262D;border:1px solid #30363D;border-radius:20px;
                 padding:5px 14px;font-size:12px;color:#8B949E;'>
      ⚙️ {st.session_state.partitions}/12 partitions
    </span>
  </div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# KPI CARDS
# ──────────────────────────────────────────────
if latest is not None:
    wl     = latest['workload']
    wl_col = "#F85149" if wl > 85 else "#D29922" if wl > 60 else "#3FB950"
    st_col = "#F85149" if latest['state'] == "CRITICAL" else "#3FB950"
    st_icon= "🚨" if latest['state'] == "CRITICAL" else "✅"
    anom_c = st.session_state.anomaly_total
    parts  = st.session_state.partitions
    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Packets</div>
        <div class="kpi-value" style="color:#58A6FF;">{st.session_state.packets_total:,}</div>
        <div class="kpi-sub">Processed by engine</div>
        <span class="kpi-badge badge-blue">LIVE</span>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Workload Index</div>
        <div class="kpi-value" style="color:{wl_col};">{wl:.1f}</div>
        <div class="kpi-sub">Current CPU load score</div>
        <span class="kpi-badge {'badge-red' if wl>85 else 'badge-yellow' if wl>60 else 'badge-green'}">
          {'HIGH' if wl>85 else 'MEDIUM' if wl>60 else 'LOW'}
        </span>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Anomalies Detected</div>
        <div class="kpi-value" style="color:#F85149;">{anom_c}</div>
        <div class="kpi-sub">By Isolation Forest</div>
        <span class="kpi-badge {'badge-red' if anom_c>0 else 'badge-green'}">
          {'DETECTED' if anom_c>0 else 'NONE YET'}
        </span>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Kafka Partitions</div>
        <div class="kpi-value" style="color:#58A6FF;">{parts}</div>
        <div class="kpi-sub">Auto-scaled / max 12</div>
        <span class="kpi-badge {'badge-red' if parts>=8 else 'badge-yellow' if parts>=5 else 'badge-green'}">
          {parts}/12 ACTIVE
        </span>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">System State</div>
        <div class="kpi-value" style="color:{st_col};font-size:22px;">{st_icon} {latest['state']}</div>
        <div class="kpi-sub">Active: {latest['device']} · {latest['window']}</div>
        <span class="kpi-badge {'badge-red' if latest['state']=='CRITICAL' else 'badge-green'}">
          {latest['timestamp']}
        </span>
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div style='background:#161B22;border:1px solid #30363D;border-radius:12px;
                padding:30px;text-align:center;margin-bottom:20px;'>
      <div style='font-size:15px;color:#8B949E;'>⏳ Starting engine — collecting first data points...</div>
    </div>
    """, unsafe_allow_html=True)


# ──────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📡  Live Stream",
    "🧠  ML Detection",
    "⚙️  Auto-Scaling",
    "📋  Data Logs",
    "📊  Analytics",
    "💻  System Monitor",
    "🔴  Critical Apps",
])


# ══════════════════════════════════════════════
# TAB 1 — LIVE STREAM
# ══════════════════════════════════════════════
with tab1:
    c1, c2 = st.columns([3, 2], gap="large")
    with c1:
        st.markdown('<div class="sec-title">📡 Live Kafka Stream — Workload / CPU / Data Rate</div>', unsafe_allow_html=True)
        if len(buf) >= 2:
            chart_df = buf.tail(chart_pts)[['workload','cpu','data_rate']].reset_index(drop=True).astype(float)
            chart_df.columns = ['Workload Index', 'CPU %', 'Data Rate MB/s']
            st.line_chart(chart_df, y=['Workload Index', 'CPU %', 'Data Rate MB/s'],
                          height=280, use_container_width=True)
        else:
            st.markdown('<div style="padding:60px;text-align:center;color:#8B949E;">⏳ Collecting data stream...</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="sec-title">🔁 Auto-Scaling Status</div>', unsafe_allow_html=True)
        if latest is not None:
            wl = latest['workload']
            if latest['state'] == 'CRITICAL':
                st.markdown('<div class="alert-red">🚨 CRITICAL — Anomaly detected, rerouting traffic</div>', unsafe_allow_html=True)
            elif wl > 75:
                st.markdown('<div class="alert-yellow">⚠️ HIGH LOAD — Scaling up Kafka partitions</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="alert-green">✅ STABLE — Load within optimal range</div>', unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            def prog(label, val_str, pct, color):
                st.markdown(f"""
                <div class="prog-wrap">
                  <div class="prog-row"><span>{label}</span><b>{val_str}</b></div>
                  <div class="prog-track">
                    <div class="prog-fill" style="width:{min(pct,100)}%;background:{color};"></div>
                  </div>
                </div>""", unsafe_allow_html=True)
            prog("Kafka Partitions", f"{st.session_state.partitions} / 12",
                 int((st.session_state.partitions/12)*100),
                 "#F85149" if st.session_state.partitions >= 8 else "#58A6FF")
            prog("CPU Utilisation", f"{latest['cpu']:.1f}%",
                 int(min(latest['cpu'],100)),
                 "#F85149" if latest['cpu']>80 else "#D29922" if latest['cpu']>55 else "#3FB950")
            prog("Memory Usage", f"{latest['memory']:.0f} MB",
                 int((latest['memory']/1024)*100),
                 "#F85149" if latest['memory']>850 else "#D29922" if latest['memory']>600 else "#3FB950")
            prog("Data Rate", f"{latest['data_rate']:.1f} MB/s",
                 int(min((latest['data_rate']/100)*100,100)),
                 "#F85149" if latest['data_rate']>70 else "#3FB950")
            st.markdown(f"""
            <div style='margin-top:14px;padding:10px 14px;background:#21262D;
                        border-radius:8px;border:1px solid #30363D;
                        font-family:JetBrains Mono,monospace;font-size:11.5px;color:#8B949E;'>
              Device: <span style='color:#58A6FF;font-weight:600;'>{latest['device']}</span>
              &nbsp;&nbsp;Window: <span style='color:#E6EDF3;'>{latest['window']}</span>
              &nbsp;&nbsp;Time: <span style='color:#E6EDF3;'>{latest['timestamp']}</span>
            </div>
            """, unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 2 — ML DETECTION
# ══════════════════════════════════════════════
with tab2:
    st.markdown('<div class="sec-title">🧠 Isolation Forest — Real-Time Anomaly Detection</div>', unsafe_allow_html=True)
    if len(buf) >= 5:
        total    = len(buf)
        anom_cnt = (buf['state'] == 'CRITICAL').sum()
        norm_cnt = total - anom_cnt
        rate     = (anom_cnt / total * 100) if total > 0 else 0
        st.markdown(f"""
        <div style='display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:20px;'>
          <div class="kpi-card"><div class="kpi-label">Total Samples</div>
            <div class="kpi-value" style="color:#58A6FF;font-size:24px;">{total:,}</div></div>
          <div class="kpi-card"><div class="kpi-label">Normal Points</div>
            <div class="kpi-value" style="color:#3FB950;font-size:24px;">{norm_cnt:,}</div></div>
          <div class="kpi-card"><div class="kpi-label">Anomalies Found</div>
            <div class="kpi-value" style="color:#F85149;font-size:24px;">{anom_cnt:,}</div></div>
          <div class="kpi-card"><div class="kpi-label">Anomaly Rate</div>
            <div class="kpi-value" style="color:#D29922;font-size:24px;">{rate:.1f}%</div></div>
        </div>
        """, unsafe_allow_html=True)
        col_info, col_chart2 = st.columns([1, 2], gap="large")
        with col_info:
            st.markdown(f"""
            <div class="panel">
              <div class="sec-title">Model Details</div>
              <table style='width:100%;font-size:12px;border-collapse:collapse;'>
                <tr><td style='color:#8B949E;padding:6px 0;'>Algorithm</td><td style='font-weight:600;color:#E6EDF3;text-align:right;'>Isolation Forest</td></tr>
                <tr><td style='color:#8B949E;padding:6px 0;'>n_estimators</td><td style='font-weight:600;color:#E6EDF3;text-align:right;'>100</td></tr>
                <tr><td style='color:#8B949E;padding:6px 0;'>contamination</td><td style='font-weight:600;color:#58A6FF;text-align:right;'>{max(anomaly_rate,0.05):.2f}</td></tr>
                <tr><td style='color:#8B949E;padding:6px 0;'>Training Window</td><td style='font-weight:600;color:#E6EDF3;text-align:right;'>Last 80 pts</td></tr>
                <tr><td style='color:#8B949E;padding:6px 0;'>Mode</td><td style='font-weight:600;color:#3FB950;text-align:right;'>Online (per tick)</td></tr>
              </table>
              <div style='margin-top:16px;padding-top:12px;border-top:1px solid #21262D;'>
                <div class="sec-title">Features</div>
                <div style='font-family:JetBrains Mono,monospace;font-size:11px;color:#58A6FF;line-height:2.1;'>
                  workload_index<br>cpu_usage<br>memory_mb<br>data_rate_mb
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        with col_chart2:
            st.markdown('<div class="sec-title">Workload Trend</div>', unsafe_allow_html=True)
            if len(buf) >= 2:
                wl_ch = buf.tail(chart_pts)[['workload']].reset_index(drop=True).astype(float)
                wl_ch.columns = ['Workload Index']
                st.area_chart(wl_ch, height=200, use_container_width=True)
            anom_df = buf[buf['state'] == 'CRITICAL'].tail(10)
            if len(anom_df) > 0:
                st.markdown('<div class="sec-title" style="margin-top:12px;">Recent Anomaly Events</div>', unsafe_allow_html=True)
                st.dataframe(anom_df[['timestamp','device','workload','cpu','memory','data_rate']].reset_index(drop=True),
                             use_container_width=True, height=200)
            else:
                st.markdown('<div class="alert-green" style="margin-top:12px;">No anomalies detected yet.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-yellow">⏳ Need at least 5 data points. Please wait a moment...</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 3 — AUTO-SCALING
# ══════════════════════════════════════════════
with tab3:
    st.markdown('<div class="sec-title">⚙️ Kafka Partition Auto-Scaling Engine</div>', unsafe_allow_html=True)
    sc1, sc2 = st.columns([2, 1], gap="large")
    with sc1:
        st.markdown('<div class="sec-title">Workload Trend</div>', unsafe_allow_html=True)
        if len(buf) >= 2:
            sc_ch = buf.tail(chart_pts)[['workload']].reset_index(drop=True).astype(float)
            sc_ch.columns = ['Workload Index']
            st.bar_chart(sc_ch, height=220, use_container_width=True)
        st.markdown('<div class="sec-title" style="margin-top:16px;">Scaling Event Log</div>', unsafe_allow_html=True)
        if st.session_state.scale_events:
            ev_html = ""
            for ev in reversed(st.session_state.scale_events[-20:]):
                cls = "ev-up" if "UP" in ev else "ev-down"
                ev_html += f'<div class="{cls}">{ev}</div>'
            st.markdown(ev_html, unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#6E7681;font-size:12px;padding:8px;">No scaling events yet.</div>', unsafe_allow_html=True)
    with sc2:
        st.markdown(f"""
        <div class="panel">
          <div class="sec-title">Scaling Rules</div>
          <div style='font-size:12px;'>
            <div style='padding:12px;background:rgba(248,81,73,0.08);border-radius:8px;margin-bottom:10px;border-left:3px solid #F85149;'>
              <b style='color:#F85149;'>▲ SCALE UP</b><br>
              <span style='color:#8B949E;'>Workload &gt; 85 or CRITICAL<br>Partitions +2 (max 12)</span>
            </div>
            <div style='padding:12px;background:rgba(63,185,80,0.08);border-radius:8px;margin-bottom:10px;border-left:3px solid #3FB950;'>
              <b style='color:#3FB950;'>▼ SCALE DOWN</b><br>
              <span style='color:#8B949E;'>Workload &lt; 35 and stable<br>Partitions -1 (min 2)</span>
            </div>
            <div style='padding:12px;background:rgba(88,166,255,0.08);border-radius:8px;border-left:3px solid #58A6FF;'>
              <b style='color:#58A6FF;'>─ STABLE</b><br>
              <span style='color:#8B949E;'>No change needed</span>
            </div>
          </div>
          <div style='margin-top:16px;padding:12px;background:#21262D;border-radius:8px;text-align:center;border:1px solid #30363D;'>
            <div style='font-size:11px;color:#8B949E;margin-bottom:4px;'>CURRENT PARTITIONS</div>
            <div style='font-family:JetBrains Mono,monospace;font-size:32px;font-weight:700;color:#58A6FF;'>{st.session_state.partitions}</div>
            <div style='font-size:11px;color:#6E7681;'>out of 12 maximum</div>
          </div>
          <div style='margin-top:10px;font-size:11px;color:#6E7681;text-align:center;'>
            Total scale events: {len(st.session_state.scale_events)}
          </div>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 4 — DATA LOGS
# ══════════════════════════════════════════════
with tab4:
    st.markdown('<div class="sec-title">📋 Process Window Logs</div>', unsafe_allow_html=True)
    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        filter_state  = st.selectbox("Filter by State",  ["All", "STABLE", "CRITICAL"])
    with fc2:
        filter_device = st.selectbox("Filter by Device", ["All"] + [f"sensor_{i}" for i in range(1,6)])
    with fc3:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        show_raw = st.checkbox("Raw Table View")

    display_df = buf.copy()
    if filter_state  != "All": display_df = display_df[display_df['state']  == filter_state]
    if filter_device != "All": display_df = display_df[display_df['device'] == filter_device]
    for col in ['workload','cpu','memory','data_rate']:
        display_df[col] = pd.to_numeric(display_df[col], errors='coerce')

    if show_raw:
        if len(display_df) > 0:
            render_df = display_df[['timestamp','device','workload','cpu','memory','data_rate','window','state']].tail(max_logs).reset_index(drop=True)
            st.dataframe(render_df, use_container_width=True,
                         height=min(440, max(200, len(render_df)*35+38)),
                         column_config={
                             "timestamp":  st.column_config.TextColumn("Time",       width="small"),
                             "device":     st.column_config.TextColumn("Device",     width="small"),
                             "workload":   st.column_config.NumberColumn("Workload",  format="%.1f", width="small"),
                             "cpu":        st.column_config.NumberColumn("CPU %",     format="%.1f", width="small"),
                             "memory":     st.column_config.NumberColumn("Mem (MB)",  format="%.0f", width="small"),
                             "data_rate":  st.column_config.NumberColumn("Rate MB/s", format="%.1f", width="small"),
                             "window":     st.column_config.TextColumn("Window",      width="small"),
                             "state":      st.column_config.TextColumn("State",       width="small"),
                         })
        else:
            st.markdown('<div style="color:#8B949E;padding:20px;text-align:center;">No data matches selected filters.</div>', unsafe_allow_html=True)
    else:
        logs_html = ""
        for _, row in display_df.tail(max_logs).iloc[::-1].iterrows():
            is_anom = row['state'] == 'CRITICAL'
            st_span = f'<span class="log-bad">● CRITICAL</span>' if is_anom else f'<span class="log-ok">● STABLE</span>'
            logs_html += f"""
            <div class="log-row {'anom' if is_anom else ''}">
              <span class="log-ts">{row['timestamp']}</span>
              <span class="log-dev">{row['device']}</span>
              <span class="log-val">WL: {row['workload']:.1f}</span>
              <span class="log-val">CPU: {row['cpu']:.1f}%</span>
              <span class="log-val">MEM: {row['memory']:.0f}MB</span>
              <span class="log-val">DR: {row['data_rate']:.1f}MB/s</span>
              {st_span}
              <span style='color:#6E7681;font-size:11px;margin-left:auto;'>{row['window']}</span>
            </div>"""
        if logs_html:
            st.markdown(logs_html, unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#8B949E;padding:20px;text-align:center;">No data matches selected filters.</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div style='font-size:11px;color:#6E7681;margin-top:8px;'>
      Showing {min(max_logs, len(display_df))} of {len(display_df)} filtered records
      &nbsp;·&nbsp; Total buffer: {len(buf)} packets
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 5 — ANALYTICS
# ══════════════════════════════════════════════
with tab5:
    st.markdown('<div class="sec-title">📊 Session Analytics</div>', unsafe_allow_html=True)
    if len(buf) >= 5:
        an1, an2 = st.columns(2, gap="large")
        with an1:
            st.markdown('<div class="sec-title">Device Activity (Packet Count)</div>', unsafe_allow_html=True)
            dev_c = buf['device'].value_counts().reset_index()
            dev_c.columns = ['Device', 'Packets']
            st.bar_chart(dev_c.set_index('Device'), height=200)
            st.markdown('<div class="sec-title" style="margin-top:16px;">State Distribution</div>', unsafe_allow_html=True)
            st_dist = buf['state'].value_counts()
            total_rows = len(buf)
            dist_html = ""
            for state_name, count in st_dist.items():
                pct = count / total_rows * 100
                color = "#F85149" if state_name == "CRITICAL" else "#3FB950"
                dist_html += f"""
                <div style='background:#0D1117;border-radius:8px;padding:12px 14px;margin-bottom:8px;border:1px solid #21262D;'>
                  <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;'>
                    <span style='font-weight:600;color:{color};font-family:JetBrains Mono,monospace;font-size:13px;'>{state_name}</span>
                    <span style='color:#E6EDF3;font-weight:700;font-size:14px;'>{count:,}
                      <span style='color:#6E7681;font-size:11px;font-weight:400;'>({pct:.1f}%)</span></span>
                  </div>
                  <div style='height:6px;background:#21262D;border-radius:3px;'>
                    <div style='height:100%;width:{pct}%;background:{color};border-radius:3px;'></div>
                  </div>
                </div>"""
            st.markdown(dist_html, unsafe_allow_html=True)
        with an2:
            st.markdown('<div class="sec-title">Summary Statistics</div>', unsafe_allow_html=True)
            stats = buf[['workload','cpu','memory','data_rate']].astype(float).describe().round(2)
            col_labels = {'workload':'Workload','cpu':'CPU %','memory':'Memory MB','data_rate':'Data Rate'}
            stat_names = {'count':'Count','mean':'Mean','std':'Std Dev','min':'Min',
                          '25%':'25th %','50%':'Median','75%':'75th %','max':'Max'}
            tbl = '<div style="background:#0D1117;border-radius:10px;overflow:hidden;border:1px solid #21262D;">'
            tbl += '<table style="width:100%;border-collapse:collapse;font-size:12px;font-family:JetBrains Mono,monospace;">'
            tbl += '<thead><tr style="background:#161B22;">'
            tbl += '<th style="padding:10px 12px;text-align:left;color:#8B949E;font-weight:600;border-bottom:1px solid #21262D;">Stat</th>'
            for col in ['workload','cpu','memory','data_rate']:
                tbl += f'<th style="padding:10px 12px;text-align:right;color:#58A6FF;font-weight:600;border-bottom:1px solid #21262D;">{col_labels[col]}</th>'
            tbl += '</tr></thead><tbody>'
            for i, (stat, row) in enumerate(stats.iterrows()):
                bg = "#161B22" if i % 2 == 0 else "#0D1117"
                tbl += f'<tr style="background:{bg};">'
                tbl += f'<td style="padding:9px 12px;color:#8B949E;font-weight:600;">{stat_names.get(stat,stat)}</td>'
                for col in ['workload','cpu','memory','data_rate']:
                    c = "#F85149" if stat=='max' and col in ['workload','cpu'] else "#58A6FF" if stat=='mean' else "#E6EDF3"
                    tbl += f'<td style="padding:9px 12px;text-align:right;color:{c};">{row[col]}</td>'
                tbl += '</tr>'
            tbl += '</tbody></table></div>'
            st.markdown(tbl, unsafe_allow_html=True)

        st.markdown('<div class="sec-title" style="margin-top:20px;">Anomalies per Device</div>', unsafe_allow_html=True)
        anom_dev = buf[buf['state']=='CRITICAL']['device'].value_counts().reset_index()
        anom_dev.columns = ['Device','Anomaly Count']
        if len(anom_dev) > 0:
            st.bar_chart(anom_dev.set_index('Device'), height=180)
        else:
            st.markdown('<div class="alert-green">No anomalies recorded yet.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-yellow">⏳ Analytics will appear after a few seconds of data collection.</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════
# TAB 6 — SYSTEM MONITOR
# Uses st.fragment with run_every=3 for flicker-free refresh
# ══════════════════════════════════════════════
with tab6:
    @st.fragment(run_every=3)
    def render_system_monitor():
        sinfo = get_system_info()
        st.markdown(f"""
        <div class="sys-grid-4">
          <div class="sys-kpi">
            <div class="kpi-label">Operating System</div>
            <div style='font-size:15px;font-weight:700;color:#58A6FF;margin:5px 0;'>{sinfo['os']}</div>
            <div class="kpi-sub">Host: {sinfo['node']}</div>
            <div class="kpi-sub">Arch: {sinfo['machine']}</div>
          </div>
          <div class="sys-kpi">
            <div class="kpi-label">CPU Usage</div>
            <div class="kpi-value" style="color:{'#F85149' if sinfo['cpu_percent']>80 else '#D29922' if sinfo['cpu_percent']>50 else '#3FB950'};">{sinfo['cpu_percent']:.1f}%</div>
            <div class="kpi-sub">{sinfo['cpu_count']} logical cores</div>
          </div>
          <div class="sys-kpi">
            <div class="kpi-label">RAM Usage</div>
            <div class="kpi-value" style="color:{'#F85149' if sinfo['mem_percent']>80 else '#D29922' if sinfo['mem_percent']>60 else '#3FB950'};">{sinfo['mem_percent']:.1f}%</div>
            <div class="kpi-sub">{sinfo['mem_used']:.1f} / {sinfo['mem_total']:.1f} GB</div>
          </div>
          <div class="sys-kpi">
            <div class="kpi-label">Disk · Battery</div>
            <div class="kpi-value" style="color:{'#F85149' if sinfo['disk_percent']>85 else '#D29922' if sinfo['disk_percent']>65 else '#3FB950'};font-size:24px;">{sinfo['disk_percent']:.1f}%</div>
            <div class="kpi-sub">{sinfo['disk_used']:.1f}/{sinfo['disk_total']:.1f} GB &nbsp;·&nbsp; 🔋 {sinfo['battery']}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        rb1, rb2 = st.columns(2, gap="large")
        with rb1:
            st.markdown('<div class="sec-title">📊 Resource Load</div>', unsafe_allow_html=True)
            def sp(label, pct, txt, c):
                st.markdown(f"""
                <div class="prog-wrap">
                  <div class="prog-row"><span>{label}</span><b>{txt} ({pct:.1f}%)</b></div>
                  <div class="prog-track"><div class="prog-fill" style="width:{min(pct,100)}%;background:{c};"></div></div>
                </div>""", unsafe_allow_html=True)
            cpu_c = "#F85149" if sinfo['cpu_percent']>80 else "#D29922" if sinfo['cpu_percent']>50 else "#3FB950"
            mem_c = "#F85149" if sinfo['mem_percent']>80 else "#D29922" if sinfo['mem_percent']>60 else "#3FB950"
            dsk_c = "#F85149" if sinfo['disk_percent']>85 else "#D29922" if sinfo['disk_percent']>65 else "#3FB950"
            sp("🖥️ CPU",  sinfo['cpu_percent'],  f"{sinfo['cpu_percent']:.1f}%", cpu_c)
            sp("🧠 RAM",  sinfo['mem_percent'],  f"{sinfo['mem_used']:.1f} GB",  mem_c)
            sp("💾 Disk", sinfo['disk_percent'], f"{sinfo['disk_used']:.1f} GB", dsk_c)
            sp("🔁 Swap", sinfo['swap_percent'], f"{sinfo['swap_used']:.0f} MB", "#58A6FF")
            st.markdown(f"""
            <div style='margin-top:14px;padding:12px;background:#0D1117;border-radius:8px;border:1px solid #21262D;'>
              <div style='font-size:11px;color:#6E7681;margin-bottom:6px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;'>🌐 Network I/O</div>
              <div style='display:flex;gap:24px;font-family:JetBrains Mono,monospace;font-size:12px;'>
                <div>⬆ Sent: <span style='color:#58A6FF;font-weight:600;'>{sinfo['net_sent']:.1f} MB</span></div>
                <div>⬇ Recv: <span style='color:#3FB950;font-weight:600;'>{sinfo['net_recv']:.1f} MB</span></div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        with rb2:
            st.markdown('<div class="sec-title">🔝 Top 8 Processes by CPU</div>', unsafe_allow_html=True)
            top8 = sinfo['processes'][:8]
            rows = ""
            for p in top8:
                c = p['CPU %']
                cls = "sp-hi" if c>20 else "sp-md" if c>5 else "sp-lo"
                bc  = "sb-hi" if c>20 else "sb-md" if c>5 else "sb-lo"
                lbl = "HIGH" if c>20 else "MED" if c>5 else "LOW"
                nm  = (p['Application'][:24]+'…') if len(p['Application'])>24 else p['Application']
                rows += f"""
                <div class="sys-proc-row {cls}">
                  <span class="sp-pid">PID {p['PID']}</span>
                  <span class="sp-name">{nm}</span>
                  <span class="sp-cpu">CPU {c:.1f}%</span>
                  <span class="sp-mem">{p['Memory (MB)']:.0f} MB</span>
                  <span class="sp-badge {bc}">{lbl}</span>
                </div>"""
            st.markdown(rows, unsafe_allow_html=True)

        st.markdown('<div class="sec-title" style="margin-top:18px;">📋 All Running Applications</div>', unsafe_allow_html=True)
        fa, fb = st.columns([3,2])
        with fa:
            search = st.text_input("🔍 Search app", placeholder="chrome, python, explorer…", key="sysmon_search", label_visibility="collapsed")
        with fb:
            flt = st.selectbox("Filter", ["All","🔴 HIGH CPU (>20%)","🟡 MED CPU (5–20%)","🟢 LOW CPU (<5%)","🔺 High Memory (>500MB)"], key="sysmon_filter", label_visibility="collapsed")

        procs = sinfo['processes']
        if search:
            procs = [p for p in procs if search.lower() in p['Application'].lower()]
        if "HIGH CPU" in flt:    procs = [p for p in procs if p['CPU %'] > 20]
        elif "MED CPU" in flt:   procs = [p for p in procs if 5 <= p['CPU %'] <= 20]
        elif "LOW CPU" in flt:   procs = [p for p in procs if p['CPU %'] < 5]
        elif "High Memory" in flt: procs = [p for p in procs if p['Memory (MB)'] > 500]

        rows2 = ""
        for p in procs[:40]:
            c = p['CPU %']
            cls = "sp-hi" if c>20 else "sp-md" if c>5 else "sp-lo"
            bc  = "sb-hi" if c>20 else "sb-md" if c>5 else "sb-lo"
            lbl = "HIGH" if c>20 else "MED" if c>5 else "LOW"
            nm  = (p['Application'][:28]+'…') if len(p['Application'])>28 else p['Application']
            rows2 += f"""
            <div class="sys-proc-row {cls}">
              <span class="sp-pid">PID {p['PID']}</span>
              <span class="sp-name">{nm}</span>
              <span class="sp-cpu">CPU {c:.1f}%</span>
              <span class="sp-mem">{p['Memory (MB)']:.0f} MB</span>
              <span style='color:#6E7681;font-size:10px;min-width:60px;'>{p['Status']}</span>
              <span class="sp-badge {bc}">{lbl}</span>
            </div>"""
        if rows2:
            st.markdown(rows2, unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#8B949E;padding:14px;text-align:center;">No processes match.</div>', unsafe_allow_html=True)

        st.markdown(f"""
        <div style='font-size:11px;color:#6E7681;margin-top:6px;'>
          Showing {min(40,len(procs))} of {len(procs)} filtered &nbsp;·&nbsp;
          Total: {len(sinfo['processes'])} &nbsp;·&nbsp;
          <span style='color:#3FB950;'>Auto-refreshing every 3 s</span>
        </div>""", unsafe_allow_html=True)

    render_system_monitor()


# ══════════════════════════════════════════════
# TAB 7 — CRITICAL APPS
# FIX: Build the ENTIRE HTML block as one string first, then
# call st.markdown() ONCE with unsafe_allow_html=True.
# Never embed large HTML blobs inside an outer f-string —
# Streamlit's sanitiser strips tags found in interpolated vars.
# ══════════════════════════════════════════════
with tab7:
    @st.fragment(run_every=3)
    def render_critical_apps():
        sinfo = get_system_info()
        procs = sinfo['processes']

        CPU_CRIT = 15
        MEM_CRIT = 400
        CPU_WARN = 5
        MEM_WARN = 150

        critical = [p for p in procs if p['CPU %'] > CPU_CRIT or p['Memory (MB)'] > MEM_CRIT]
        warning  = [p for p in procs if
                    (CPU_WARN <= p['CPU %'] <= CPU_CRIT or MEM_WARN <= p['Memory (MB)'] <= MEM_CRIT)
                    and p not in critical]

        critical.sort(key=lambda x: x['CPU %'] + x['Memory (MB)']/100, reverse=True)
        warning.sort(key=lambda x: x['CPU %'] + x['Memory (MB)']/100, reverse=True)

        total_cpu = sum(p['CPU %'] for p in procs)
        total_mem = sum(p['Memory (MB)'] for p in procs)

        # ── helper: escape HTML special chars in process names ──
        def _esc(s):
            return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

        # ── Build rows for critical column ──
        def _crit_rows(items, limit=25):
            if not items:
                return '<div class="alert-green">&#x2705; No critical apps right now.</div>'
            rows = ""
            for p in items[:limit]:
                nm = _esc((p['Application'][:22]+'…') if len(p['Application'])>22 else p['Application'])
                why_parts = []
                if p['CPU %'] > CPU_CRIT:       why_parts.append(f"CPU {p['CPU %']:.1f}%")
                if p['Memory (MB)'] > MEM_CRIT: why_parts.append(f"MEM {p['Memory (MB)']:.0f}MB")
                why = _esc(" · ".join(why_parts))
                rows += (
                    '<div class="crit-row">'
                    f'<span class="cr-pid">PID {p["PID"]}</span>'
                    f'<span class="cr-name crit-name">{nm}</span>'
                    f'<span class="cr-cpu">{p["CPU %"]:.1f}%</span>'
                    f'<span class="cr-mem">{p["Memory (MB)"]:.0f} MB</span>'
                    f'<span class="cr-why cr-why-crit">&#9888; {why}</span>'
                    '</div>'
                )
            return rows

        # ── Build rows for warning column ──
        def _warn_rows(items, limit=25):
            if not items:
                return '<div class="alert-green">&#x2705; No warnings at the moment.</div>'
            rows = ""
            for p in items[:limit]:
                nm = _esc((p['Application'][:22]+'…') if len(p['Application'])>22 else p['Application'])
                why_parts = []
                if CPU_WARN <= p['CPU %'] <= CPU_CRIT:       why_parts.append(f"CPU {p['CPU %']:.1f}%")
                if MEM_WARN <= p['Memory (MB)'] <= MEM_CRIT: why_parts.append(f"MEM {p['Memory (MB)']:.0f}MB")
                why = _esc(" · ".join(why_parts) or "Elevated")
                rows += (
                    '<div class="warn-row">'
                    f'<span class="cr-pid">PID {p["PID"]}</span>'
                    f'<span class="cr-name warn-name">{nm}</span>'
                    f'<span class="cr-cpu">{p["CPU %"]:.1f}%</span>'
                    f'<span class="cr-mem">{p["Memory (MB)"]:.0f} MB</span>'
                    f'<span class="cr-why cr-why-warn">&#9889; {why}</span>'
                    '</div>'
                )
            return rows

        # ── Assemble the full HTML block as a plain string ──
        left_title  = f'&#128308; Critical ({len(critical)}) &mdash; CPU &gt;{CPU_CRIT}% or Mem &gt;{MEM_CRIT}MB'
        right_title = f'&#128993; Warning ({len(warning)}) &mdash; CPU {CPU_WARN}&ndash;{CPU_CRIT}% or Mem {MEM_WARN}&ndash;{MEM_CRIT}MB'

        full_html = (
            # ── banner ──
            '<div class="crit-header">'
            '<div style="font-size:28px;">&#128308;</div>'
            '<div style="min-width:0;flex:1;">'
            '<div style="font-size:15px;font-weight:700;color:#F85149;">Critical App Monitor</div>'
            '<div style="font-size:12px;color:#8B949E;margin-top:2px;">'
            f'{len(critical)} critical &nbsp;&middot;&nbsp; {len(warning)} warning &nbsp;&middot;&nbsp; '
            f'{len(procs)} total &nbsp;&middot;&nbsp; '
            f'CPU: <b style="color:#E6EDF3;">{total_cpu:.1f}%</b> &nbsp;&middot;&nbsp; '
            f'Mem: <b style="color:#E6EDF3;">{total_mem/1024:.1f} GB</b>'
            '</div></div>'
            '<div style="font-size:11px;color:#3FB950;white-space:nowrap;">&#128260; Auto-refresh 3 s</div>'
            '</div>'

            # ── two-column grid ──
            '<div class="crit-grid">'

            # left col — critical
            '<div class="crit-col">'
            f'<div class="sec-title">{left_title}</div>'
            + _crit_rows(critical) +
            '</div>'

            # right col — warning
            '<div class="crit-col">'
            f'<div class="sec-title">{right_title}</div>'
            + _warn_rows(warning) +
            '</div>'

            '</div>'  # end crit-grid
        )

        # ── Single markdown call — no interpolated HTML blobs ──
        st.markdown(full_html, unsafe_allow_html=True)

        # ── CPU bar chart ──
        if critical or warning:
            st.markdown(
                '<div class="sec-title" style="margin-top:20px;">&#128202; CPU &mdash; Critical &amp; Warning Processes</div>',
                unsafe_allow_html=True
            )
            chart_procs = (critical + warning)[:20]
            if chart_procs:
                cdf = pd.DataFrame(chart_procs)[['Application', 'CPU %']].copy()
                cdf['Application'] = cdf['Application'].str[:18]
                cdf = cdf.set_index('Application')
                st.bar_chart(cdf[['CPU %']], height=220, use_container_width=True)

    render_critical_apps()


# ──────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────
st.markdown("""
<div style='text-align:center;padding:16px;margin-top:24px;
            border-top:1px solid #21262D;font-size:11px;color:#484F58;'>
  Real-Time Data Processing Engine using Apache Kafka &nbsp;·&nbsp;
  Alina Pervaiz · 2024MCSECS019 &nbsp;·&nbsp;
  NIT Srinagar CSE &nbsp;·&nbsp;
  Dr. Shaima Qureshi &amp; Dr. Mahreen Salim
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# AUTO-REFRESH — only Kafka pipeline, fragments handle system UI
# ──────────────────────────────────────────────
if st.session_state.running:
    time.sleep(refresh_rate)
    st.rerun()