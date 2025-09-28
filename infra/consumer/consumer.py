import json
import boto3
import time
from kafka import KafkaConsumer

# MinIO connection
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)

bucket_name = "bronze-transactions"

# Define the consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consumer streaming and saving it to MinIO...")

# Loop and save data
for message in consumer:
    record = message.value
    symbol = record.get("symbol")
    ts = record.get("fetched_at", int(time.time()))  # fixed
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,  # fixed
        Body=json.dumps(record),
        ContentType="application/json"
    )

    print(f"Saved record for {symbol} → s3://{bucket_name}/{key}")
