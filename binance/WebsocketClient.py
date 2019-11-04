import json
from websocket import create_connection
from threading import Thread
import time
from datetime import datetime, timedelta

class WebsocketClient():

    last_update_date = datetime.now()

    def __init__(self, pairs, ws_url="wss://stream.binance.com:9443/stream?streams=",subprotocols=["binary","base64"]):
        #bnbusdt@bookTicker
        streams_pairs = [f"{x.lower()}@bookTicker" for x in pairs]
        self.stop = False
        self.url = ws_url + "/".join(streams_pairs)
        self.ws = create_connection(self.url)

    def sub(self):
        self.open()
        # subParams = json.dumps(topics)
        # self.ws.send(subParams)
        self.listen()

    def open(self):
        print("-- Subscribed! --")

    def listen(self):
        while not self.stop:
            if self.last_update_date + timedelta(seconds=30) < datetime.now():
                # now_ms = int(time.time())
                self.ws.pong('pong')
                print('Sent a PING!!')
                self.last_update_date = datetime.now()
            try:
                msg = json.loads(self.ws.recv())
            except Exception as e:
                print(e)
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
