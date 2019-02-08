''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from csv        import DictReader
from pyspark    import SparkConf, SparkContext
import redis
from subprocess import call, check_output
from time       import time
from uuid       import uuid4
import os
# from pyspark.sql import SQLContext


def nightly_update(input_tuple):
    spark_conf = SparkConf().setAppName("Batch processing")

def original_input(input_tuple):
    overall_start_time = time()

    redis_db = redis.Redis(host="10.0.0.13", port=6379, db=2)
    redis_db.flushdb()

    print('\nFile size:', input_tuple[1])
    cur_start_time = time()
    dataFile = sc.textFile('s3n://eric-ford-insight-19/original/' + input_tuple[0]) # Don't forget it's s3n, not s3.
    header   = dataFile.first()
    print('Time to read file:          ', round(time() - cur_start_time, 2), 'secs.')
    print('Number of partitions:       ', dataFile.getNumPartitions())

    cur_start_time = time()
    # create initial key:val
    # keyed_data       = dataFile.map(create_map_keys_fn)
    keyed_data       = dataFile.filter(lambda line: line != header).map(create_map_keys_fn)
    # count appearances of each key in `keyed_data`
    r = keyed_data.first()
    print('Time to create original map:', round(time() - cur_start_time, 2), 'secs.')
    print('Number of partitions:       ', keyed_data.getNumPartitions())

    cur_start_time = time()
    # keyed_for_counts = dataFile.map(map_counts_fn)
    keyed_for_counts = dataFile.filter(lambda line: line != header).map(map_counts_fn)
    # get count of each user's reviews
    counts           = keyed_for_counts.reduceByKey(count_keys)
    r = counts.first()
    print('Time to count keys:         ', round(time() - cur_start_time, 2), 'secs.')
    print('Number of partitions:       ', counts.getNumPartitions())

    cur_start_time = time()
    averages         = keyed_data.reduceByKey(average_reviews)
    final            = counts.join(averages).map(concat_fn)
    r = final.first()
    print('Time to join counts:        ', round(time() - cur_start_time, 2), 'secs.')
    print('Number of partitions:       ', final.getNumPartitions())
    cur_start_time = time()
    final.foreachPartition( redis_insert )
    print('time to write to Redis:     ', round(time() - cur_start_time,2), 'secs.')

    print('\nTotal time:                 ', round(time() - overall_start_time,2), 'secs.' )

    print()
    headers = ['user:', 'number of reviews:', 'average star rating:', 'average review length:']
    for item in final.take(10):
        print( '{} {:<12} {} {:<2}   {} {:>}    {} {:>}'.format( headers[0], item[0],
                                                                 headers[1], item[1],
                                                                 headers[2], round(item[2]/item[1], 2),
                                                                 headers[3], round(item[3]/item[1], 2)
                                                               )
             )
    print()


def clear_s3_directories(input_list):
    ''' Steps through S3 bucket and removes any directoris and directory pointers, because script will hang
        if it attempts to write into directory that already exists. '''
    # for loop is in case I decide to have multiple outputs later
    def file_exists(filename):
        return check_output(['aws', 's3', 'ls', 's3://eric-ford-insight-19/'], universal_newlines=True).find(output_file + filename) >= 0
    for output_file in input_list:
        if file_exists(''):
            call(['aws', 's3', 'rm', 's3://eric-ford-insight-19/spark_output', '--recursive'])
        # This because aws cli fails in an ugly way if a specified file is missing. So I check for it.
        if file_exists('_$folder$'):
            call(['aws', 's3', 'rm', 's3://eric-ford-insight-19/spark_output_$folder$'])


def redis_insert(iter):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    pass
    # from sys import exit
    # redis_db = redis.Redis(host="10.0.0.13", port=6379, db=1)
    # a = open('countfiles/' + str(uuid4()), 'w')
    # a.close()
    # print(tup)
    # print(tup[0], 'num', tup[1][0], 'stars', tup[1][1], 'words', tup[1][2] )
    # for tup in iter:
        # print(tup)
        # exit(1)
        # if redis_db.exists(tup[0]):

            # redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )
        # else:
        # print(list(tup[1:]))
        # redis_db.lpush(tup[0], *list(tup[1:]))
            # redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )


def create_map_keys_fn(line):
    ''' Return a tuple with key : val = user_id : [star rating, number of words]. '''
    line = line.split('\t')
    return line[1], [int(line[7]), line[13].count(' ') + 1]


def map_counts_fn(line):
    ''' Create a tuple of key : val as user_id : 1. This to be used later for counting number of times
        each keys shows up in map. '''
    line = line.split('\t') # there's got to be a faster way
    return line[1], 1

def concat_fn(line):
    ''' Receive a tuple of form (key, []) a Return a tuple with . '''
    return line[0], line[1][0], line[1][1][0], line[1][1][1]


def average_reviews(accum, input_list):
    return [accum[0] + input_list[0], accum[1] + input_list[1]]


def count_keys(accum, input_value):
    ''' Increment each time it's called. '''
    return accum + 1


spark_conf = SparkConf().setAppName("Batch processing") #("spark.cores.max", "1")
sc         = SparkContext(conf=spark_conf)



for filename_tuple in [ ('amazon_reviews_us_Digital_Software_v1_00.tsv.gz',       '18MB'),
                        ('amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz',    '184MB'),
                        ('amazon_reviews_us_Apparel_v1_00.tsv.gz',                '620MB'),
                        ('amazon_reviews_us_Books_v1_02.tsv.gz',                  '1.2GB'),
                        ('amazon_reviews_us_Wireless_v1_00.tsv.gz',               '1.6GB'),
                        ('amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz', '2.5GB'),
                      ]:
    original_input(filename_tuple)

# if(__name__ == "__main__"):
#     original_input()


