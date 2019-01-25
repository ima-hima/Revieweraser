#! /bin/bash

cd spark
rm -rf spark_output
spark-submit \
    --jars /home/ubuntu/Insight-DE-2019A-Project/pyspark-cassandra-0.9.0.jar \
    --py-files /home/ubuntu/Insight-DE-2019A-Project/pyspark-cassandra-0.9.0.jar \
    --conf spark.cassandra.connection.host=10.0.0.1 \
    --master local \
    spark_run.py

# spark-submit spark_run.py
