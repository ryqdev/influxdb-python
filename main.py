import json
import os, time
import yfinance as yf
import pandas as pd

from influxdb_client_3 import InfluxDBClient3, Point
from loguru import logger
from typing import NoReturn

import websocket

TOKEN = os.environ.get("INFLUX_TOKEN")
ORG = os.environ.get("INFLUX_ORG")
HOST = os.environ.get("INFLUX_URL")
BUCKET = os.environ.get("INFLUX_BUCKET")
SYMBOL = "BTCUSDT"


def init_client() -> InfluxDBClient3:
  return InfluxDBClient3(host=HOST, token=TOKEN, org=ORG)

def influx_write(price: str) -> NoReturn:
  client = init_client()
  point = (
    Point(SYMBOL)
    .field("Price", price)
  )
  client.write(database=BUCKET, record=point)
  logger.info("Complete. Return to the InfluxDB UI.")

def on_message(ws, message):
    j = json.loads(message)
    data = j.get("data")
    price = data[0].get("p")
    logger.info(price)
    influx_write(price)

def on_error(ws, error):
    logger.info(error)

def on_close(ws):
    logger.info("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')


def run():
  websocket.enableTrace(True)
  ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=cg9cnopr01qk68o82q1gcg9cnopr01qk68o82q20",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
  ws.on_open = on_open
  ws.run_forever()

if __name__ == "__main__":
   run()