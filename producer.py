import pandas as pd
import json
import time
from kafka import KafkaProducer

# Connect to the local Kafka Docker container
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Reading PaySim Dataset (Loading first 10,000 rows for testing)...")
# We load a subset first to ensure the pipeline works without crashing your RAM
df = pd.read_csv('paysim.csv', nrows=10000) 

print("Streaming transactions to Kafka topic: 'transactions_input'...")
for index, row in df.iterrows():
    # Convert the row into a JSON dictionary
    transaction = row.to_dict()
    
    # Send the transaction to Kafka
    producer.send('transactions_input', value=transaction)
    
    print(f"Sent Transaction: {transaction['nameOrig']} -> ${transaction['amount']}")
    
    # Pause for 0.1 seconds to simulate real-time streaming (10 transactions/sec)
    time.sleep(0.1)
