from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='category_stats_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Słownik automatycznie tworzący strukturę statystyk dla nowej kategorii
category_stats = defaultdict(lambda: {'count': 0, 'sum': 0.0, 'min': float('inf'), 'max': float('-inf')})

total_msgs_processed = 0
current_batch_count = 0

print("Rozpoczęto śledzenie statystyk per kategoria (tumbling window: 10 wiadomości)...")

for message in consumer:
    transaction = message.value
    
    category = transaction.get('category', 'Nieznana')
    amount = transaction.get('amount', 0.0)
    
    # 1. Pobranie i aktualizacja statystyk dla danej kategorii w bieżącym oknie
    stats = category_stats[category]
    stats['count'] += 1
    stats['sum'] += amount
    
    if amount < stats['min']:
        stats['min'] = amount
    if amount > stats['max']:
        stats['max'] = amount
        
    current_batch_count += 1
    total_msgs_processed += 1
    
    # 2. Sprawdzenie, czy okno się wypełniło
    if current_batch_count == 10:
        print(f"\n--- Statystyki dla ostatnich 10 wiadomości (łącznie przetworzono: {total_msgs_processed}) ---")
        print(f"{'Kategoria':<15} | {'Liczba':<8} | {'Przychód':<12} | {'Min':<8} | {'Max':<8}")
        print("-" * 61)
        
        for cat, data in category_stats.items():
            # Zabezpieczenie przed dziwnym formatowaniem, gdyby min/max zostało jako nieskończoność
            # (choć w tym przypadku dla wyczyszczonego słownika count będzie > 0)
            print(f"{cat:<15} | {data['count']:<8} | {data['sum']:<12.2f} | {data['min']:<8.2f} | {data['max']:<8.2f}")
            
        # 3. KLUCZOWY KROK: Resetowanie okna (tumbling window)
        category_stats.clear()
        current_batch_count = 0
