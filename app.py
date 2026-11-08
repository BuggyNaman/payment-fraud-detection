import streamlit as st
from kafka import KafkaConsumer
import json
import time

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Real-Time Payment Monitoring 🛡️")

# Create a placeholder in the UI that we can constantly overwrite with new data
placeholder = st.empty()

st.sidebar.header("System Status")
st.sidebar.success("Connected to Kafka broker (localhost:9092)")
st.sidebar.info("Listening for live transactions...")

# Connect to the Kafka topic
# 'auto_offset_reset='latest'' ensures we only see new transactions as they happen
try:
    consumer = KafkaConsumer(
        'transactions_input', 
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest' 
    )
    
    # Continuously read messages from Kafka
    for message in consumer:
        data = message.value
        
        # Update the UI inside our placeholder
        with placeholder.container():
            st.subheader(f"Transaction Type: {data.get('type', 'UNKNOWN')}")
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Amount", f"${data.get('amount', 0):,.2f}")
            col2.metric("Sender", data.get('nameOrig', 'Unknown'))
            col3.metric("Receiver", data.get('nameDest', 'Unknown'))
            
            # A temporary basic rule until the Machine Learning model is connected
            if data.get('amount', 0) > 500000:
                st.error("🚨 ALERT: High Value Transaction Detected!")
            else:
                st.success("✅ Transaction processed successfully.")
                
        # Tiny sleep to ensure the browser UI can keep up with the render speed
        time.sleep(0.05) 

except Exception as e:
    st.error(f"Waiting for Kafka stream... (Ensure Docker and producer.py are running). Error: {e}")
