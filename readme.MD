# deployed on - `https://share.streamlit.io/`
## 🎉Live Running on - `https://kafka---real-time-data-processing-engine-uocsgcmtej2jdm83jnw82.streamlit.app/` [Click Me](https://kafka---real-time-data-processing-engine-uocsgcmtej2jdm83jnw82.streamlit.app/)

# ⚡ Real-Time Data Processing Engine using Apache Kafka

**Candidate:** Alina Pervaiz | 2024MCSECS019
**Institute:** NIT Srinagar — CSE Department | MTech 3rd Semester
**Supervisors:** Dr. Shaima Qureshi & Dr. Mahreen Salim

---

## Overview

This project implements a real-time data processing dashboard that simulates Apache Kafka stream processing concepts. It monitors live sensor data, detects anomalies using Machine Learning, and auto-scales Kafka partitions based on workload — all visualized through an interactive web dashboard built with Streamlit.

---

## How to Run

```bash
streamlit run app/main.py
```

The dashboard opens automatically in your browser at `http://localhost:8501`

---

## Project Structure

```
project/
├── app/
│   └── main.py        ← Complete application (single file)
└── README.md
└── requirements.txt
```

---

## Core Concepts

### Apache Kafka (Simulated)
Kafka is a distributed event streaming platform. In this project, the following Kafka concepts are implemented:

| Concept | Implementation |
|---------|---------------|
| **Producer** | Simulated sensors (sensor_1 to sensor_5) generating data |
| **Consumer** | Dashboard consuming and visualizing the stream |
| **Partitions** | Auto-scaled from 2 to 12 based on workload |
| **Sliding Window** | Fixed-size windows (W-000, W-001...) for batch processing |
| **Stream Processing** | Per-tick ML inference on incoming data |

### Isolation Forest (ML Model)
An unsupervised anomaly detection algorithm from scikit-learn. It works by isolating data points — anomalies are easier to isolate (require fewer splits in a decision tree), so they get lower anomaly scores.

- **Training window:** Last 80 data points (online learning)
- **Features:** workload, cpu, memory, data_rate
- **Output:** STABLE (normal) or CRITICAL (anomaly)

---

## Dashboard Sections

### Tab 1 — Live Stream

**What it shows:** Real-time line chart of three metrics streaming from simulated sensors.

**Data Flow:**
```
tick counter increments
  → sensor selected round-robin: sensor_1 to sensor_5
  → workload = 25 + 20*sin(tick/25) + noise_or_spike
  → cpu = workload + small_noise (clamped 5 to 100%)
  → memory = 150 to 550 MB (normal) or 750 to 950 MB (spike)
  → data_rate = 3 to 28 MB/s (normal) or 55 to 95 MB/s (spike)
  → plotted on line chart (last N points, configurable)
```

**Metrics displayed:**
- **Workload Index** — Composite CPU load score
- **CPU %** — Simulated CPU utilization percentage
- **Data Rate MB/s** — Simulated data throughput

**Right panel — Auto-Scaling Status:**
- Color-coded alert (green/yellow/red)
- Progress bars for Partitions, CPU, Memory, Data Rate
- Active device and window information

---

### Tab 2 — ML Detection

**What it shows:** Isolation Forest model running live on each incoming data point.

**Data Flow:**
```
new data point arrives (workload, cpu, memory, data_rate)
  → Isolation Forest trains on last 80 buffer entries
  → model.predict([[workload, cpu, memory, data_rate]])
  → returns +1 (STABLE) or -1 (CRITICAL)
  → if CRITICAL: anomaly_total counter increments
  → state saved with the record in buffer
```

**Model Configuration:**

| Parameter | Value |
|-----------|-------|
| Algorithm | Isolation Forest |
| n_estimators | 100 |
| contamination | slider value (default 0.08) |
| Training window | Last 80 data points |
| Mode | Online — retrained every tick |

**Displays:**
- 4 KPI cards: Total Samples, Normal Points, Anomalies Found, Anomaly Rate %
- Workload area chart (trend over session)
- Table of last 10 anomaly events

---

### Tab 3 — Auto-Scaling

**What it shows:** Kafka partition auto-scaling decisions based on workload thresholds.

**Scaling Logic:**
```
if workload > 85 OR state == "CRITICAL":
    partitions = min(partitions + 2, 12)     Scale UP

elif workload < 35 AND partitions > 2:
    partitions = max(partitions - 1, 2)      Scale DOWN

else:
    no change                                 Stable
```

**Data Flow:**
```
after every tick → check workload + state
  → apply scaling rule
  → if partition count changed → log event with timestamp + workload
  → event log displayed in reverse chronological order
```

**Displays:**
- Workload bar chart (history)
- Scaling event log with timestamps
- Scaling rules reference panel
- Current partition count (out of max 12)

---

### Tab 4 — Data Logs

**What it shows:** Every processed data packet stored in the session buffer, with filtering.

**Data Flow:**
```
every tick → new row appended to buffer (max 500 rows, FIFO)
  → user selects filter: All / STABLE / CRITICAL
  → user selects filter: All / sensor_1 to sensor_5
  → filtered records displayed (most recent first)
```

**Record Fields:**

| Field | Description |
|-------|-------------|
| timestamp | HH:MM:SS of packet arrival |
| device | sensor_1 through sensor_5 |
| workload | Workload index at that tick |
| cpu | CPU % at that tick |
| memory | RAM usage in MB |
| data_rate | Data rate in MB/s |
| window | Sliding window ID (W-000, W-001...) |
| state | STABLE or CRITICAL |

**Two view modes:** Styled log rows (color-coded) or Raw DataFrame table

---

### Tab 5 — Analytics

**What it shows:** Statistical summary of the entire session buffer.

**Data Flow:**
```
buffer (up to 500 records) → pandas groupby + describe()
  → device activity count (bar chart)
  → state distribution (STABLE vs CRITICAL %)
  → summary statistics (mean, std, min, max, quartiles)
  → anomalies per device (bar chart)
```

**Statistics computed:**

| Metric | Fields |
|--------|--------|
| Count | Total records per device |
| Mean | Average workload, CPU, memory, data rate |
| Std Dev | Variation in each metric |
| Min / Max | Range of each metric |
| Quartiles | 25th, 50th, 75th percentiles |

---

### Tab 6 — System Monitor

**What it shows:** Real hardware metrics from the host machine using psutil. Not simulated — actual system data.

**Data Flow:**
```
@st.fragment(run_every=3) — independent 3-second refresh
  → psutil.cpu_percent(interval=None) — non-blocking
  → psutil.virtual_memory()
  → psutil.disk_usage('/')
  → psutil.process_iter([...]) — all running processes
  → @st.cache_data(ttl=2) — cached 2s to prevent CPU spike
  → displayed in monitor panels
```

**System data collected:**

| Metric | Source |
|--------|--------|
| OS and Architecture | platform.uname() |
| CPU % | psutil.cpu_percent(interval=None) |
| RAM used / total | psutil.virtual_memory() |
| Disk used / total | psutil.disk_usage('/') |
| Battery | psutil.sensors_battery() |
| Network sent / recv | psutil.net_io_counters() |
| All processes | psutil.process_iter() |

**Key detail:** interval=None used for non-blocking CPU measurement. With interval=1, the call freezes the UI for 1 second on every refresh.

**Process color coding:**
- RED — CPU > 20% (HIGH)
- YELLOW — CPU 5 to 20% (MED)
- GREEN — CPU < 5% (LOW)

---

### Tab 7 — Critical Apps

**What it shows:** Real system processes classified as Critical or Warning based on resource consumption.

**Data Flow:**
```
same psutil data as System Monitor (shared cache)
  → classify each process:
    CRITICAL:  CPU% > 15  OR  Memory > 400 MB
    WARNING:   CPU% 5 to 15  OR  Memory 150 to 400 MB
               (not already in CRITICAL)
  → sort by (CPU% + Memory/100) descending
  → render as two-column HTML grid
  → @st.fragment(run_every=3) — refreshes independently
```

**Why CPU can show more than 100%:**
On a multi-core system, CPU % is reported per-core. An 8-core machine can show 800% total. System Idle Process shows the unused CPU capacity across all cores.

**Layout fix:** The two-column layout is rendered as a single HTML string instead of st.columns() to prevent layout mismatch when the sidebar opens or closes.

---

## Sidebar Controls

### Engine Controls
| Button | Action |
|--------|--------|
| Pause / Start | Halt or resume data generation |
| Reset | Clear all data, restart counters |

### Parameter Sliders
| Slider | Range | Default | Effect |
|--------|-------|---------|--------|
| Anomaly Probability | 2% to 30% | 8% | Likelihood of injecting a spike |
| Refresh Speed | 0.3s to 3.0s | 0.7s | How often the Kafka pipeline ticks |
| Window Size | 5 to 30 | 10 | Sliding window packet count |
| Log Rows Shown | 5 to 40 | 15 | Rows displayed in Data Logs tab |
| Chart History | 20 to 150 | 60 | Data points shown in charts |

### Download Options
- **Full Stream Data (CSV)** — Entire session buffer
- **Anomalies Only (CSV)** — Filtered CRITICAL records
- **Session Report (TXT)** — Summary statistics plus scaling log

---

## Technical Architecture

### Zero-Flicker Refresh

**Problem:** Streamlit's st.rerun() applies a CSS fade animation to all elements, causing the entire dashboard to dim briefly on every refresh.

**Solution — Three-layer approach:**

1. **CSS override** — Disables Streamlit's internal fade animation on stVerticalBlock
2. **st.fragment(run_every=3)** — System Monitor and Critical Apps refresh independently without a full-page rerun
3. **st.cache_data(ttl=2)** — System calls cached for 2 seconds, preventing repeated expensive psutil calls

### HTML Rendering Fix

Streamlit sanitizes HTML embedded inside f-string interpolated variables. The fix is to build the entire HTML block as a single flat string and call st.markdown() exactly once with unsafe_allow_html=True.

### Session State

All live state persists across reruns via st.session_state:

| Key | Type | Description |
|-----|------|-------------|
| tick | int | Current pipeline tick count |
| running | bool | Engine start or pause state |
| anomaly_total | int | Cumulative anomaly count |
| packets_total | int | Total packets processed |
| partitions | int | Current Kafka partition count |
| scale_events | list | Log of all scaling decisions |
| buffer | DataFrame | Rolling 500-row data buffer |

---

## Dependencies

```bash
pip install streamlit pandas numpy scikit-learn psutil
```

---

## Key Engineering Decisions

| Decision | Reason |
|----------|--------|
| Isolation Forest (unsupervised) | No labelled training data needed for streaming anomaly detection |
| Online retraining every tick | Adapts to changing baseline — realistic for production systems |
| interval=None for CPU % | Non-blocking — prevents 1-second UI freeze on every refresh |
| Single-file architecture | Simpler deployment, no inter-module dependencies |
| HTML string concatenation | Avoids Streamlit markdown sanitizer stripping injected HTML |
| st.fragment over st.rerun for system tabs | Partial page update — no full-page flash |

---

*NIT Srinagar — CSE Department | MTech 3rd Semester*
*Supervisors: Dr. Shaima Qureshi and Dr. Mahreen Salim*