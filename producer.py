import pandas as pd
import json
import time
from kafka import KafkaProducer

# Connect to the local Kafka Docker container
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# print("Reading PaySim Dataset (Loading first 10,000 rows for testing)...")
# # We load a subset first to ensure the pipeline works without crashing your RAM
# df = pd.read_csv('paysim.csv', nrows=10000) 
print("Reading and shuffling the PaySim Dataset...")
# 1. Read the full dataset so we don't miss the rare fraud rows
df = pd.read_csv('paysim.csv')

# 2. Shuffle the entire dataset randomly
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# 3. Keep only the first 10,000 rows of the newly shuffled data to save memory
df = df.head(10000)
print("Streaming transactions to Kafka topic: 'transactions_input'...")
import os

print("Starting the live stream (Awaiting UI command)...")

for index, row in df.iterrows():
    # 1. Check the control file from the UI
    while True:
        if os.path.exists("control.txt"):
            with open("control.txt", "r") as f:
                status = f.read().strip()
        else:
            status = "STOPPED" # Default state before you click start in UI
        
        if status == "RUNNING":
            break # Exit the wait loop and send the data
        
        time.sleep(0.5) # Wait half a second and check again if still STOPPED
    
    # 2. Convert and Send the transaction
    transaction = row.to_dict()
    producer.send('transactions_input', value=transaction)
    
    print(f"Sent Transaction: {transaction['nameOrig']} -> ${transaction['amount']}")
    time.sleep(0.1)