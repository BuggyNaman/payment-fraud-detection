# import streamlit as st
# import pandas as pd
# import json
# from kafka import KafkaConsumer
# import time

# st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
# st.title("🛡️ Real-Time Payment Fraud Detection")

# # Initialize Kafka Consumer
# @st.cache_resource
# def create_consumer():
#     return KafkaConsumer(
#         'predictions_output',
#         bootstrap_servers=['localhost:9092'],
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#         auto_offset_reset='latest'
#     )

# consumer = create_consumer()

# # UI Placeholders for real-time updates
# metrics_placeholder = st.empty()
# table_placeholder = st.empty()

# # Initialize session state for storing historical data
# if 'data' not in st.session_state:
#     st.session_state.data = []

# st.markdown("### Live Transaction Feed")

# # Polling loop to update the dashboard
# for message in consumer:
#     transaction = message.value
#     st.session_state.data.append(transaction)
    
#     # Keep only the last 100 transactions in memory for UI performance
#     if len(st.session_state.data) > 100:
#         st.session_state.data.pop(0)
        
#     df = pd.DataFrame(st.session_state.data)
    
#     # Calculate live metrics
#     total_tx = len(df)
#     fraud_tx = len(df[df['prediction'] == 1.0])
#     fraud_rate = (fraud_tx / total_tx) * 100 if total_tx > 0 else 0
    
#     # Update UI Metrics
#     with metrics_placeholder.container():
#         col1, col2, col3 = st.columns(3)
#         col1.metric("Total Transactions", total_tx)
#         col2.metric("Fraudulent Transactions", fraud_tx)
#         col3.metric("Current Fraud Rate", f"{fraud_rate:.2f}%")
    
#     # Update UI Table
#     with table_placeholder.container():
#         # Highlight fraudulent rows in red
#         def highlight_fraud(row):
#             return ['background-color: #ffcccc' if row['prediction'] == 1.0 else '' for _ in row]
        
#         st.dataframe(df.style.apply(highlight_fraud, axis=1), use_container_width=True)
        
#     time.sleep(0.1) # Small pause to prevent UI flickering
import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import time
import os

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CSS INJECTION ---
st.markdown("""
<style>
    /* Hide Streamlit default branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* App Background */
    .stApp {
        background-color: #0b0f19;
        color: #ffffff;
    }
    
    /* Dashboard Title */
    h1 {
        font-family: 'Helvetica Neue', sans-serif;
        font-weight: 700;
        padding-bottom: 20px;
    }
    
    /* Metric Cards Styling */
    div[data-testid="metric-container"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0px 4px 12px rgba(0,0,0,0.5);
        text-align: center;
    }
    
    /* Metric Label */
    div[data-testid="metric-container"] label {
        color: #8b949e !important;
        font-size: 1.1rem !important;
        font-weight: 500;
    }
    
    /* Metric Value */
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #58a6ff;
        font-size: 2.5rem;
        font-weight: 700;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #161b22;
        border-right: 1px solid #30363d;
    }
    
    /* Modernized Buttons */
    .stButton>button {
        border: 1px solid #30363d;
        border-radius: 6px;
        background-color: #21262d;
        color: #c9d1d9;
        font-weight: 600;
        width: 100%;
        transition: all 0.2s ease-in-out;
    }
    .stButton>button:hover {
        border-color: #8b949e;
        background-color: #30363d;
    }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ Real-Time Payment Fraud Detection")

# --- UI CONTROL FOR PRODUCER ---
st.sidebar.header("Stream Controls")

if not os.path.exists("control.txt"):
    with open("control.txt", "w") as f:
        f.write("STOPPED")

col1, col2 = st.sidebar.columns(2)
if col1.button("▶️ Start Stream"):
    with open("control.txt", "w") as f:
        f.write("RUNNING")
if col2.button("⏸️ Pause Stream"):
    with open("control.txt", "w") as f:
        f.write("STOPPED")

with open("control.txt", "r") as f:
    current_status = f.read().strip()
    
# Style the status indicator
status_color = "#3fb950" if current_status == "RUNNING" else "#f85149"
st.sidebar.markdown(f"Producer Status: <strong style='color:{status_color}'>{current_status}</strong>", unsafe_allow_html=True)
st.sidebar.markdown("---")

# --- INITIALIZE GLOBAL COUNTERS & LOCAL TABLE ---
if 'global_total_tx' not in st.session_state:
    st.session_state.global_total_tx = 0
if 'global_fraud_tx' not in st.session_state:
    st.session_state.global_fraud_tx = 0
if 'recent_data' not in st.session_state:
    st.session_state.recent_data = []

@st.cache_resource
def create_consumer():
    return KafkaConsumer(
        'predictions_output',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

consumer = create_consumer()

metrics_placeholder = st.empty()
table_placeholder = st.empty()

st.markdown("### Live Transaction Feed (Latest 100 Rows)")

# --- LIVE POLLING LOOP ---
while True:
    # Safely poll Kafka for new messages in batches (waits up to 100ms)
    msg_pack = consumer.poll(timeout_ms=100)
    
    for tp, messages in msg_pack.items():
        for message in messages:
            transaction = message.value
            
            st.session_state.global_total_tx += 1
            if transaction.get('prediction') == 1.0:
                st.session_state.global_fraud_tx += 1
                
            st.session_state.recent_data.append(transaction)
            if len(st.session_state.recent_data) > 100:
                st.session_state.recent_data.pop(0)
                
    # Only redraw the UI if new data actually arrived
    if msg_pack:
        df = pd.DataFrame(st.session_state.recent_data)
        fraud_rate = (st.session_state.global_fraud_tx / st.session_state.global_total_tx) * 100 if st.session_state.global_total_tx > 0 else 0
        
        with metrics_placeholder.container():
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Transactions", f"{st.session_state.global_total_tx:,}")
            st.markdown("""<style>div[data-testid="metric-container"]:nth-child(2) div[data-testid="stMetricValue"] {color: #f85149;}</style>""", unsafe_allow_html=True)
            m2.metric("Fraudulent Transactions", f"{st.session_state.global_fraud_tx:,}")
            m3.metric("Global Fraud Rate", f"{fraud_rate:.3f}%")
        
        with table_placeholder.container():
            def highlight_fraud(row):
                return ['background-color: #8B0000; color: white; font-weight: bold' if row.get('prediction') == 1.0 else '' for _ in row]
            
            st.dataframe(df.style.apply(highlight_fraud, axis=1), use_container_width=True)
            
    time.sleep(0.05)