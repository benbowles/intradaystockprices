
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.resolve()))
from scode import *
from pyspark.sql.types import *

def add_max(window):

    col = 'max_' + str(window)

    return_schema = StructType([
            StructField("Timestamp", IntegerType(), True),
            StructField("ticker", StringType(), True),
            StructField(col, FloatType(), True),
    ])
    add_feature_wrapper(lambda pdf: add_max_rolling(pdf, window, col),
                        return_schema, col, test=False)


if __name__ == '__main__':
    for window in [10, 20, 50, 75, 100, 150, 200]:
        add_max(window)
