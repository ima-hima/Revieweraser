#! /bin/bash

export PYSPARK_PYTHON=python3

rm -rf spark_output
spark-submit --master spark://10.0.0.4:7077 --executor-cores 6 --executor-memory 5GB ./prototyping.py

# executor-cores is number of cores each executor gets.
# executor-memory is total memory each executor gets.

# This is old information:
# My cluster had 1 master and 3 workers, each worker had 6 cores and total of 6.8GB memory.
# If I set cores = 6 and memory = 5GB, I had all cores in use and 1.8GB overhead for each CPU.
# I then had four executors running
