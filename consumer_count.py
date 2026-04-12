from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
total_msgs_processed = 0
current_batch_count = 0

print("Rozpoczęto zliczanie statystyk per sklep (paczki po 10 wiadomości)...")

for message in consumer:
    transaction = message.value
    
    store = transaction.get('store', 'Nieznany')
    amount = transaction.get('amount', 0)
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0) + amount
    
    current_batch_count += 1
    total_msgs_processed += 1
    
    if current_batch_count == 10:
        print(f"\n--- Podsumowanie dla ostatnich 10 wiadomości (łącznie: {total_msgs_processed}) ---")
        print(f"{'Sklep':<15} | {'Liczba':<8} | {'Suma':<10} | {'Średnia':<10}")
        print("-" * 52)
        
        for s, count in store_counts.items():
            suma = total_amount[s]
            srednia = suma / count if count > 0 else 0
            print(f"{s:<15} | {count:<8} | {suma:<10.2f} | {srednia:<10.2f}")

        store_counts.clear()
        total_amount.clear()
        current_batch_count = 0
