#! /bin/bash

export PYSPARK_PYTHON=python3

rm -rf spark_output
spark-submit --master spark://10.0.0.4:7077 --executor-memory 5g --executor-memory 5g src/testing/profiling.py

# spark-submit spark_run.py
