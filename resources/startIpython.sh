(mkdir -p /Users/bowles/stocks/.temp/container && \
   cp -r /Users/bowles/stocks/scode /Users/bowles/stocks/.temp/container && cd \
    /Users/bowles/stocks/scode /Users/bowles/stocks/.temp/container && zip -r - .)  > /Users/bowles/stocks/.temp/scode.zip \
    && PYSPARK_DRIVER_PYTHON=ipython  pyspark --packages io.delta:delta-core_2.12:2.1.1 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.submit.pyFiles=/Users/bowles/scode.zip" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
    --driver-memory 20g --py-files /Users/bowles/scode.zip