import threading
import websocket
import time


def on_message(ws, message):
    print("Message reçu")
    print(f"Message reçu : {message}")


def on_error(ws, error):
    print(f"Erreur : {error}")


def on_close(ws, close_status_code, close_msg):
    print("Connexion fermée")


def on_open(ws):
    print("Connexion ouverte")


if __name__ == "__main__":
    print("Starting WebSocket client...")
    ws = websocket.WebSocketApp(
        "ws://localhost:8888",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Exécuter le client WebSocket dans un thread
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

    try:
        while True:
            time.sleep(1)  # Sleep to prevent high CPU usage
    except KeyboardInterrupt:
        print("Interruption par l'utilisateur")
        ws.close()
        wst.join()  # Ensure the thread finishes before exiting
