from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='velocity_anomaly_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)
TIME_WINDOW_SEC = 60
MAX_TRANSACTIONS = 3

print(f"Nasłuchiwanie anomalii: > {MAX_TRANSACTIONS} transakcje w ciągu {TIME_WINDOW_SEC}s...")

for message in consumer:
    transaction = message.value
    user_id = transaction.get('user_id')
    if not user_id:
        continue
    current_time = time.time()
    user_history[user_id].append(current_time)

    user_history[user_id] = [
        t for t in user_history[user_id] 
        if current_time - t <= TIME_WINDOW_SEC
    ]
    if len(user_history[user_id]) > MAX_TRANSACTIONS:
        ilosc = len(user_history[user_id])
        print(f"[ALERT] Użytkownik {user_id} wykonał {ilosc} transakcji w ciągu ostatnich 60 sekund!")
