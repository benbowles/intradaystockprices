
#create_plugin:
#	(mkdir -p /Users/bowles/stocks/.temp/container && \
#cp -r /Users/bowles/stocks/scode /Users/bowles/stocks/.temp/container && cd \
#/Users/bowles/stocks/scode /Users/bowles/stocks/.temp/container && zip -r - .)  > /Users/bowles/stocks/.temp/scode.zip

ipython:
	PYSPARK_DRIVER_PYTHON=ipython ${SPARK_HOME}/bin/pyspark --packages io.delta:delta-core_2.12:2.1.1 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
    --driver-memory 20g

load_diff:
	${SPARK_HOME}/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.1 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
--driver-memory 20g scripts/load_diff.py


start_scratch:
	${SPARK_HOME}/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.1 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
--driver-memory 20g scripts/start_scratch.py


ticker-analytics:
	${SPARK_HOME}/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.1 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
--driver-memory 20g scripts/ticker_analytics.py

rsync:
	rsync -avze "ssh" --delete --exclude '.git' --exclude 'delta18' /Users/bowles/stocks/ Session-f1ae8cfe-5c4e-4975-8dd7-80bdcd1d7b11://sensei-fs/users/bowles/dims

script-%:
	${SPARK_HOME}/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.1 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  \
--driver-memory 20g scripts/$*.py

catchup: start_scratch script-maxmin


