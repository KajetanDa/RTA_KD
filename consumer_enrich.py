from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='risk_group', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("Rozpoczęto wzbogacanie transakcji o poziom ryzyka...")

for message in consumer:
    dane = message.value
    amount = dane.get('amount', 0)
    
    if amount > 3000:
        dane['risk_level'] = "HIGH"
    elif amount > 1000:
        dane['risk_level'] = "MEDIUM"
    else:
        dane['risk_level'] = "LOW"
        
    print(dane)
    
