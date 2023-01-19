import os.path
import pathlib

from ..imports import *

START_DATE = "2012-05-16"
START_TIME_DT = dt.datetime.strptime(START_DATE, '%Y-%m-%d').date()
CURRENT_DIR = "/Users/bowles/stocks"
DB_LOCATION = os.path.join(CURRENT_DIR, "delta10")
VENDOR_LOCATION = os.path.join(CURRENT_DIR, ".temp/vendor")
TRADING_SECS = 60 * 60 * 6.5
