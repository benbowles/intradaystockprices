from pyspark.sql import SparkSession

import time
import numpy as np
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
import datetime as dt 
from pyspark.sql.functions import explode
from scipy.ndimage import gaussian_filter
from datetime import timedelta
from xone import calendar as xone_calendar

import requests, json
from io import BytesIO
from tqdm import tqdm
from delta.tables import *
import random
from pyspark.sql.functions import lit
import traceback
from more_itertools import chunked
import matplotlib.pyplot as plt
import pandas_market_calendars as mcal

from collections import defaultdict
import numpy as np
import copy
from scipy.ndimage import gaussian_filter1d
import pandas as pd
from time import sleep
from random import random
from multiprocessing.pool import Pool
import time
from functools import partial
from itertools import islice
import concurrent
import pandas as pd
import copy
from concurrent.futures import ProcessPoolExecutor, as_completed

import random
import numpy as np
import mplfinance as mpf
from tqdm.contrib.concurrent import process_map
from functools import lru_cache
from datetime import date, timedelta

import glob
START_TIME = START_DATE = '2012-05-16'
import calendar