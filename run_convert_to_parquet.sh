#! /bin/bash

export PYSPARK_PYTHON=python3

rm -rf spark_output
spark-submit --master spark://10.0.0.4:7077 src/testing/convert_to_parquet.py

# spark-submit spark_run.py