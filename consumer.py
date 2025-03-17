import json
import mysql.connector
from kafka import KafkaConsumer

# ✅ Connect to MySQL
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="titli@18",
        database="google_trends_db"
    )
    cursor = db.cursor()
    print("✅ Connected to MySQL")
except mysql.connector.Error as err:
    print(f"❌ MySQL Error: {err}")
    exit(1)

# ✅ Connect to Kafka
try:
    consumer = KafkaConsumer(
        "layoffs",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset="earliest",  # Start from the beginning
    )
    print("✅ Connected to Kafka")
except Exception as e:
    print(f"❌ Kafka Connection Error: {e}")
    exit(1)

# 🔄 Consume messages and insert into MySQL
for message in consumer:
    try:
        data = message.value
        print(f"📩 Received Message: {data}")  # Debugging

        query = """
            INSERT INTO google_trends_data (source, timestamp, layoffs, unemployment, recession)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            data["source"],
            data["timestamp"],
            data["trends"]["layoffs"],
            data["trends"]["unemployment"],
            data["trends"]["recession"]
        )

        cursor.execute(query, values)
        db.commit()
        print(f"✅ Inserted row: {values}")

    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        db.rollback()
