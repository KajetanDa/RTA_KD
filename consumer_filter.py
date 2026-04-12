from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    dane = message.value
    amount = dane.get('amount', 0)
    if amount > 3000:
        tx_id = dane.get('tx_id', 'Brak ID')
        city = dane.get('store', 'Nieznane miasto')
        category = dane.get('category', 'Brak kategorii')
        
        print(f"ALERT: {tx_id} | {amount} PLN | {city} | {category}")
