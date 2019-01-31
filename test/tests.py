''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from csv        import DictReader
from pyspark    import SparkConf, SparkContext
from subprocess import call, check_output
from time       import time
# from pyspark.sql import SQLContext


def only_read_gz(input_filename = 'amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz'):
    start_time = time()
    spark_conf = SparkConf().setAppName("Batch processing") #("spark.cores.max", "1")
    sc         = SparkContext(conf=spark_conf)

    dataFile = sc.textFile('s3n://eric-ford-insight-19/original/' + input_filename) # Don't forget it's s3n, not s3.
    header   = dataFile.first()

    keyed_data       = dataFile.filter(lambda line: line != header).map(lambda x: (x, 1))
    # count appearances of each key in `keyed_data`

    print time() - start_time


only_read_gz()
