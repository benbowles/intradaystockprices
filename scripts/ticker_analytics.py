
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.resolve()))


from scode import *
get_ticker_summaries(test=False).show(2000)