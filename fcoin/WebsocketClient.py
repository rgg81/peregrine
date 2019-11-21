import json
from websocket import create_connection
from threading import Thread
import time
from datetime import datetime, timedelta

class WebsocketClient():

    last_update_date = datetime.now()

    def __init__(self, ws_url="wss://api.fcoin.com/v2/ws",subprotocols=["binary","base64"]):
        self.stop = False
        self.url = ws_url
        self.ws = create_connection(self.url)

    def sub(self,topics):
        self.open()
        subParams = json.dumps(topics)
        self.subParams = subParams
        self.ws.send(subParams)
        self.listen()

    def open(self):
        print("-- Subscribed! --")

    def listen(self):
        while not self.stop:
            if self.last_update_date + timedelta(seconds=30) < datetime.now():
                now_ms = int(time.time())
                ping_message = {"cmd": "ping", "args": [now_ms], "id": "rgg81"}
                self.ws.send(json.dumps(ping_message))
                print('Sent a PING!!')
                self.last_update_date = datetime.now()
            try:
                msg = json.loads(self.ws.recv())
            except Exception as e:
                print(e)
                print(f"Trying to reconnect..")
                self.ws.connect(self.url)
                self.ws.send(self.subParams)
                continue
            else:
                self.handle(msg)


    def handle(self, msg):
        #inherit method
        print(msg)

    def close(self):
        self.ws.close()
        self.closed()

    def closed(self):
        print("Socket Closed")
