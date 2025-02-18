import websocket
import json
import threading
import signal
from datetime import datetime
from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock"

# Finnhub API key
API_KEY = "cuchjr9r01qri16ntb2gcuchjr9r01qri16ntb30"


symbols = ["AAPL", "TSLA", "GOOGL", "META"] 


stop_streaming = False


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

def on_message(ws, message):

    global stop_streaming
    data = json.loads(message)

    if "data" in data:
        for entry in data["data"]:
            stock_data = {
                "timestamp": datetime.utcfromtimestamp(entry["t"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "symbol": entry["s"],
                "price": entry["p"],
                "volume": entry["v"]
            }
            
            
            producer.send(KAFKA_TOPIC, value=stock_data)
            print(f"Sent to Kafka: {stock_data}")

    if stop_streaming:
        ws.close()


def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    """Handles WebSocket closure."""
    print("WebSocket Closed")

def on_open(ws):
    """Subscribes to stock symbols when WebSocket opens."""
    for symbol in symbols:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    print("Subscribed to:", ", ".join(symbols))

def stop_streaming_signal(signum, frame):
    """Handles termination signals (Ctrl+C)."""
    global stop_streaming
    stop_streaming = True
    print("\nStopping stream...")

signal.signal(signal.SIGINT, stop_streaming_signal)

def run_websocket():
    socket_url = f"wss://ws.finnhub.io?token={API_KEY}"
    ws = websocket.WebSocketApp(socket_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()


thread = threading.Thread(target=run_websocket)
thread.start()


while True:
    command = input("Type 'exit' to stop streaming: ").strip().lower()
    if command == "exit":
        stop_streaming_signal(None, None)
        thread.join()
        producer.close()  
        print("Data streaming stopped. Exiting...")
        break
