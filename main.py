import os, time
import yfinance as yf
import pandas as pd

from influxdb_client_3 import InfluxDBClient3, Point
from loguru import logger
from typing import NoReturn


TOKEN = os.environ.get("INFLUX_TOKEN")
ORG = os.environ.get("INFLUX_ORG")
HOST = os.environ.get("INFLUX_URL")
BUCKET = os.environ.get("INFLUX_BUCKET")
SYMBOL = "OXY"


def init_client() -> InfluxDBClient3:
  return InfluxDBClient3(host=HOST, token=TOKEN, org=ORG)

def influx_write(client: InfluxDBClient3) -> NoReturn:
  data = get_data()
  for index, row in data.iterrows():
    point = (
      Point(SYMBOL)
      .tag("Time", index)
      .field("Close", row["Close"])
    )
    client.write(database=BUCKET, record=point)
  logger.info("Complete. Return to the InfluxDB UI.")

def get_data() -> pd.DataFrame:
  msft = yf.Ticker(SYMBOL)
  msft.info
  hist = msft.history(period="max")
  return hist


if __name__ == "__main__":
  client = init_client()
  influx_write(client)