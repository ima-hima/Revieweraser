#! /bin/bash

cd spark
# spark-submit \
#     --jars /home/ubuntu/Insight-DE-2019A-Project/pyspark-cassandra-0.9.0.jar \
#     --py-files /home/ubuntu/Insight-DE-2019A-Project/pyspark-cassandra-0.9.0.jar \
#     --conf spark.cassandra.connection.host=ec2-52-206-18-180.compute-1.amazonaws.com \
#     --master spark://52.206.18.180:7077 \
#     spark_run.py

spark-submit spark_run.py
