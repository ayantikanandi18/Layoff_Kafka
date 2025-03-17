import os
import json
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from pytrends.request import TrendReq
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Ensure environment variables are loaded correctly
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

if not KAFKA_BROKER or not TOPIC_NAME:
    raise ValueError("❌ ERROR: Missing Kafka environment variables. Check your .env file!")

print(f"✅ Using Kafka Broker: {KAFKA_BROKER}, Topic: {TOPIC_NAME}")

# ✅ Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Initialize Google Trends API
pytrends = TrendReq()

# 🔄 Function to Fetch and Stream Data to Kafka
def fetch_and_send_trends():
    keywords = [
        "layoffs", 
          "unemployment", "recession",
         
    ]

    print(f"🔍 Fetching Google Trends data for: {keywords}")
    
    try:
        pytrends.build_payload(kw_list=keywords, timeframe="now 7-d")  # ✅ Increased timeframe for better data
        trends_data = pytrends.interest_over_time()

        # ✅ Print raw data to debug
        print("🔍 Raw Google Trends Data:")
        print(trends_data)

        if not trends_data.empty:
            df = trends_data.drop(columns=["isPartial"], errors="ignore")  # ✅ Ignore error if column is missing
            df = df.reset_index()

            for _, row in df.iterrows():
                message = {
                    "source": "Google Trends",
                    "timestamp": str(row["date"]),
                    "trends": {keyword: row.get(keyword, 0) for keyword in keywords}  # ✅ Avoid KeyError
                }
                print("✅ Sending to Kafka:", message)  # ✅ Print before sending
                producer.send(TOPIC_NAME, message)

    except Exception as e:
        print("❌ ERROR Fetching Google Trends Data:", str(e))

# 🔄 Run the Producer in a Loop
while True:
    fetch_and_send_trends()
    time.sleep(10)  # ✅ Fetch data every 10 secs
    
