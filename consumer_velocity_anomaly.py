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

# Słownik przechowujący listy znaczników czasu transakcji dla każdego user_id
user_history = defaultdict(list)

TIME_WINDOW_SEC = 60
MAX_TRANSACTIONS = 3

print(f"Nasłuchuję na anomalie: > {MAX_TRANSACTIONS} transakcje w ciągu {TIME_WINDOW_SEC}s...")

for message in consumer:
    transaction = message.value
    user_id = transaction.get('user_id')
    
    # Pomijamy wiadomości bez user_id
    if not user_id:
        continue
        
    # Pobieramy obecny czas (czas przetwarzania)
    current_time = time.time()
    
    # 1. Dodajemy czas obecnej transakcji do historii użytkownika
    user_history[user_id].append(current_time)
    
    # 2. Usuwamy transakcje starsze niż 60 sekund (przesuwamy okno)
    user_history[user_id] = [
        t for t in user_history[user_id] 
        if current_time - t <= TIME_WINDOW_SEC
    ]
    
    # 3. Sprawdzamy, czy w oknie czasowym jest więcej transakcji niż dozwolony limit
    if len(user_history[user_id]) > MAX_TRANSACTIONS:
        ilosc = len(user_history[user_id])
        print(f"[ALERT ANOMALII] Użytkownik {user_id} wykonał {ilosc} transakcji w ciągu ostatnich 60 sekund!")
        
        # Opcjonalnie: możemy wyczyścić historię użytkownika po zgłoszeniu alertu, 
        # aby nie spamować konsoli przy każdej kolejnej transakcji w tym samym oknie.
        # user_history[user_id].clear()
