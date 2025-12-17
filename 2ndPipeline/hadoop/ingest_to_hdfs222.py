import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
from collections import defaultdict
import uuid


KAFKA_SERVER = 'kafka:29092'
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'
TOPICS = ['reddit_data', 'youtube_data', 'twitch_streams', 'tiktok_data']

def get_hdfs_client():
    print(" Connecting to HDFS...")
    while True:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.list('/') 
            print(" Connected to HDFS NameNode!")
            return client
        except Exception as e:
            print(f" HDFS not ready... retrying. ({e})")
            time.sleep(5)

def save_to_hdfs(client, topic, data_list):
    if not data_list: return
    try:
        now = datetime.now()
        folder_path = f"/datalake/{topic}/{now.year}/{now.month:02d}/{now.day:02d}"
        
        
        file_name = f"{topic}_data_{int(time.time())}_{len(data_list)}.json"
        full_path = f"{folder_path}/{file_name}"
     
        json_content = "\n".join([json.dumps(record) for record in data_list])
        
        with client.write(full_path, encoding='utf-8') as writer:
            writer.write(json_content)
            
        print(f" [HDFS] Saved {len(data_list)} records to: {full_path}")
        
    except Exception as e:
        print(f" Write Error: {e}")

def run():
    print(" Starting Permanent Data Warehouse Ingestion...")
    time.sleep(5)
    
    hdfs_client = get_hdfs_client()
    
  
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest', 
        enable_auto_commit=True, 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='social_pulse_warehouse_final_v1' 
    )

    buffers = defaultdict(list)
    BATCH_SIZE = 10 
    
    print(f" Connected to Kafka. Monitoring Topics: {TOPICS}")
    
    for message in consumer:
        data = message.value
        topic = message.topic
        
        data['ingested_at'] = datetime.now().isoformat()
        
        buffers[topic].append(data)
        
        print(f" [{topic}] received msg...")

        if len(buffers[topic]) >= BATCH_SIZE:
            save_to_hdfs(hdfs_client, topic, buffers[topic])
            buffers[topic] = [] 

if __name__ == "__main__":
    run()