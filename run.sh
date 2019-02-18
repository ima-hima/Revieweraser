#! /bin/bash

export PYSPARK_PYTHON=python3

cd src/spark
rm -rf spark_output
spark-submit --master spark://10.0.0.4:7077 --executor-cores 5 --executor-memory 6GB spark_run.py

# spark-submit spark_run.py
