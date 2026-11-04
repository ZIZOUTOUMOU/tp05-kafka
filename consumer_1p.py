import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors-1p"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-1p-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")

try:
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
        print(
               f'partition={message.partition} offset={message.offset} | '
               f'{event["sensor_id"]} | T={temperature}°C'
        )
        if temperature >= 35:   
            print("ALERT: High temperature detected!")
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    consumer.close()