import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-avg",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")
total = 0
count = 0
try:
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
        total += temperature
        count += 1
        print(f"Running average: {total / count:.2f}°C ({count} events)")
        print(
            f'{event["timestamp"]} | {event["sensor_id"]} | '
            f'T={temperature}°C | RH={event["humidity_pct"]}%'
        )
        if temperature >= 35:
            print("ALERT: High temperature detected!")
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    consumer.close()