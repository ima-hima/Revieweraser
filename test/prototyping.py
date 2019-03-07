''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from pyspark     import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from subprocess  import call, check_output
from sys         import exit
from time        import time
from uuid        import uuid4

# import boto3
# import csv
import os
import redis





def without_databricks(input_tuple, sc, numPartitions, interim_profiling):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB. '''
    overall_start_time = time()

    print('\n\n**default**')
    print('\nFile size:', input_tuple[1])
    print('Number of partitions', numPartitions)

    host, passwd, port, db = open('redis-pass.txt').readline().split()
    # print(host)
    cur_start_time = time()
    originalRDD = sc.textFile('s3a://eric-ford-insight-19/original/' + input_tuple[0]) # Don't forget it's s3a, not s3.
    # originalRDD = originalRDD.repartition(5)
    header      = originalRDD.first()
    print_updates('read file:', overall_start_time, originalRDD.getNumPartitions())

    # Here and below, call to first() is to force evaluation for profiling output. Is that slowing
    # things down? Maybe, so put in conditional.
    if interim_profiling:
        cur_start_time = time()

    # create initial key:val. After this rows will be (user_id, [star rating, number of words])
    keyed_data = originalRDD.filter(lambda line: line != header).map(create_map_keys_idx_fn)
    # keyed_data = keyed_data.repartition(5)

    if interim_profiling:
        r = keyed_data.first() # this to force lazy eval for timing
        print_updates('create original map:', cur_start_time, keyed_data.getNumPartitions())

    # Determine how many reviews each reviewer has written.
    if interim_profiling:
        cur_start_time = time()
    keyed_for_counts = originalRDD.filter(lambda line: line != header).map(map_counts_rdd_fn)
    # keyed_for_counts = keyed_for_counts.repartition(numPartitions)    # repartitioning here speeds things up significantly
    counts           = keyed_for_counts.reduceByKey(count_keys)
    if interim_profiling:
        r = counts.first() # this to force lazy eval for timing
        print_updates('count keys:', cur_start_time, keyed_for_counts.getNumPartitions())

    # Count total stars and total word count for each user, to be used eventually for sum_review_values.
    if interim_profiling:
        cur_start_time = time()
    per_review_totals = keyed_data.reduceByKey(sum_review_values)
    if interim_profiling:
        r = per_review_totals.first()
        print_updates('total counts per review:', cur_start_time, per_review_totals.getNumPartitions())

    # Now do join between per user counts and the number of reviews a user has written, so we can add to DB. After this the data should be in the form
    # (user_id, num_reviews, total_stars, total_words)
    if interim_profiling:
        cur_start_time = time()
    final = counts.join(per_review_totals).map(concat_fn)
    if interim_profiling:
        r = final.first() # this to force lazy eval for timing
        print_updates('join:', cur_start_time, counts.getNumPartitions())

    cur_start_time = time()
    final.foreachPartition( lambda x: redis_insert(x, host, passwd, port, db) )
    # for i in range(10000):
    #     redis_insert_test(i)
    print_updates('write to Redis:', cur_start_time, final.getNumPartitions())

    print('{:<29}{} {}'.format('\nTotal time:', round(time() - overall_start_time, 2), 'secs.'))

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


def without_all(input_tuple, sc, numPartitions, interim_profiling):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB. '''
    overall_start_time = time()

    print('\n\n**without all**')
    print('\nFile size:', input_tuple[1])
    print('Number of partitions', numPartitions)

    host, passwd, port, db = open('redis-pass.txt').readline().split()
    # print(host)
    cur_start_time = time()
    originalRDD = sc.textFile('s3a://eric-ford-insight-19/original/' + input_tuple[0]) # Don't forget it's s3a, not s3.
    # originalRDD = originalRDD.repartition(5)
    header      = originalRDD.first()
    print_updates('read file:', overall_start_time, originalRDD.getNumPartitions())

    # Here and below, call to first() is to force evaluation for profiling output. Is that slowing
    # things down? Maybe, so put in conditional.
    if interim_profiling:
        cur_start_time = time()

    # create initial key:val. After this rows will be (user_id, [star rating, number of words])
    keyed_data = originalRDD.filter(lambda line: line != header).map(create_map_keys_idx_fn)
    # keyed_data = keyed_data.repartition(5)

    if interim_profiling:
        r = keyed_data.first() # this to force lazy eval for timing
        print_updates('create original map:', cur_start_time, keyed_data.getNumPartitions())

    # Determine how many reviews each reviewer has written.
    if interim_profiling:
        cur_start_time = time()
    keyed_for_counts = originalRDD.filter(lambda line: line != header).map(map_counts_rdd_fn)
    keyed_for_counts = keyed_for_counts.repartition(numPartitions)    # repartitioning here speeds things up significantly
    counts           = keyed_for_counts.reduceByKey(count_keys)
    if interim_profiling:
        r = counts.first() # this to force lazy eval for timing
        print_updates('count keys:', cur_start_time, keyed_for_counts.getNumPartitions())

    # Count total stars and total word count for each user, to be used eventually for sum_review_values.
    if interim_profiling:
        cur_start_time = time()
    per_review_totals = keyed_data.reduceByKey(sum_review_values)
    if interim_profiling:
        r = per_review_totals.first()
        print_updates('total counts per review:', cur_start_time, per_review_totals.getNumPartitions())

    # Now do join between per user counts and the number of reviews a user has written, so we can add to DB. After this the data should be in the form
    # (user_id, num_reviews, total_stars, total_words)
    if interim_profiling:
        cur_start_time = time()
    final = counts.join(per_review_totals).map(concat_fn)
    if interim_profiling:
        r = final.first() # this to force lazy eval for timing
        print_updates('join:', cur_start_time, counts.getNumPartitions())

    cur_start_time = time()
    final.foreachPartition( lambda x: redis_insert_pipelining(x, host, passwd, port, db) )
    # for i in range(10000):
    #     redis_insert_test(i)
    print_updates('write to Redis:', cur_start_time, final.getNumPartitions())

    print('{:<29}{} {}'.format('\nTotal time:', round(time() - overall_start_time, 2), 'secs.'))

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

def without_join(input_tuple, sc, numPartitions, interim_profiling):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB.
        Skip the count and join steps and aggregate directly into Redis. This is about 5x slower than
        doing counts and joins in Spark. '''
    overall_start_time = time()

    print('\n\n**without join**')
    print('\nFile size:', input_tuple[1])
    print('Number of partitions', numPartitions)

    host, passwd, port, db = open('redis-pass.txt').readline().split()
    # print(host)
    cur_start_time = time()
    originalRDD = sc.textFile('s3a://eric-ford-insight-19/original/' + input_tuple[0]) # Don't forget it's s3a, not s3.
    # originalRDD = originalRDD.repartition(5)
    header      = originalRDD.first()
    print_updates('read file:', overall_start_time, originalRDD.getNumPartitions())

    # Here and below, call to first() is to force evaluation for profiling output. Is that slowing
    # things down? Maybe, so put in conditional.
    if interim_profiling:
        cur_start_time = time()

    # create initial key:val. After this rows will be (user_id, [star rating, number of words])
    keyed_data = originalRDD.filter(lambda line: line != header).map(create_map_keys_idx_fn)
    keyed_data = keyed_data.repartition(numPartitions)
    keyed_data.foreachPartition( lambda x: redis_insert_without_join(x, host, passwd, port, db) )
    # for i in range(10000):
    #     redis_insert_test(i)
    print_updates('write to Redis:', cur_start_time, keyed_data.getNumPartitions())

    print('{:<29}{} {}'.format('\nTotal time:', round(time() - overall_start_time, 2), 'secs.'))

    print()
    # headers = ['user:', 'number of reviews:', 'average star rating:', 'average review length:']
    # for item in final.take(10):
    #     print( '{} {:<12} {} {:<2}   {} {:>}    {} {:>}'.format( headers[0], item[0],
    #                                                              headers[1], item[1],
    #                                                              headers[2], round(item[2]/item[1], 2),
    #                                                              headers[3], round(item[3]/item[1], 2)
    #                                                            )
    #          )
    print()


def print_updates(what_we_did, current_time, partitions):
    ''' Print out profiling info. '''
    print('Time to {:<30}{} secs.'.format(what_we_did, round(time() - current_time, 2)))
    print('{:<38}{}'.format('Number of partitions:', partitions))


def clear_s3_directories(input_list):
    ''' Step through S3 bucket and remove any directories and directory pointers, because script will hang
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


def redis_insert(iter, host, passwd, port, db):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    # redis_db = redis.Redis(host="10.0.0.13", port=6379, db=1)
    db = redis.StrictRedis(host=host, password=passwd, port=int(port), db=int(db))
    # d = db.pipeline()
    for tup in iter:
        if db.exists(tup[0]):
            db.hincrby(tup[0], 'num',   tup[1])
            db.hincrby(tup[0], 'stars', tup[2])
            db.hincrby(tup[0], 'words', tup[3])
        else:
            db.hminsert(tup[0], {'num': tup[1], 'starts': tup[2], 'words': tup[3]})


def redis_insert_pipelining(iter, host, passwd, port, db):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    # redis_db = redis.Redis(host="10.0.0.13", port=6379, db=1)
    db = redis.StrictRedis(host=host, password=passwd, port=int(port), db=int(db))
    d = db.pipeline()
    for tup in iter:
        d.hincrby(tup[0], 'num',   tup[1])
        d.hincrby(tup[0], 'stars', tup[2])
        d.hincrby(tup[0], 'words', tup[3])


def redis_insert_without_join(iter, host, passwd, port, db):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    db = redis.StrictRedis(host=host, password=passwd, port=int(port), db=int(db))
    d = db.pipeline()
    for tup in iter:
        d.hincrby(tup[0], 'num',   1)
        d.hincrby(tup[0], 'stars', tup[1][0])
        d.hincrby(tup[0], 'words', tup[1][1])


def redis_insert_test(i):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    redis_db = redis.Redis(host="10.0.0.13", port=6379, db=0)
    # start = time()

    redis_db.hmset(i, {'num': 1, 'stars': 1, 'words': 1} )

    # print( time() - start )


def create_map_keys_fn(line):
    ''' Return a tuple (user_id, [star rating, number of words]). '''
    print(line)
    return (line['customer_id'], [int(line['star_rating']), str(line['review_body']).count(' ') + 1])


def create_map_keys_idx_fn(line):
    ''' Same as create_map_keys_fn, but using indices rather than keys. '''
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])


def map_counts_fn(line):
    ''' Create a tuple of key : val as user_id : 1. This to be used later for counting number of times
        each keys shows up in map. '''
    return line['customer_id'], 1


def map_counts_rdd_fn(line):
    ''' Same as map_counts_fn, but using indices rather than keys. '''
    line = line.split('\t') # there's got to be a faster way
    return (line[1], 1)

def concat_fn(line):
    ''' Receive a tuple of form (key, [,(,)]) a Return a tuple (,,,) . '''
    return (line[0], line[1][0], line[1][1][0], line[1][1][1])


def sum_review_values(accum, input_list):
    ''' Get the values—star rating and word count—in a review and add to the accumulator. '''
    return ([accum[0] + input_list[0], accum[1] + input_list[1]])


def count_keys(accum, input_value):
    ''' Increment each time it's called. This will be used to get a total for how many times each
        key appears. '''
    return accum + 1


def main():
    ''' Set up options for prototype and profiling code. '''
    spark_conf = SparkConf().setAppName("Revieweraser") #("spark.cores.max", "1")
    sc         = SparkContext(conf=spark_conf)

    sc = SparkContext.getOrCreate()

    sqlContext = SQLContext(sc)

    for filename_tuple in [
                            # ('spiked_data.tsv', '33kb'),
                            # ('amazon_reviews_us_Digital_Software_v1_00.tsv.gz',       '18MB'),
                            ('amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz',    '184MB'),
                            # ('amazon_reviews_us_Apparel_v1_00.tsv.gz',                '620MB'),
                            # ('amazon_reviews_us_Books_v1_02.tsv.gz',                  '1.2GB'),
                            # ('amazon_reviews_us_Wireless_v1_00.tsv.gz',               '1.6GB'),
                            # ('amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz', '2.5GB'),
                          ]:
        for numPartitions in [10]: #[1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
            without_databricks(filename_tuple, sc, numPartitions, True)
            without_all(filename_tuple, sc, numPartitions, True)
            # without_join(filename_tuple, sc, numPartitions, True)

if(__name__ == "__main__"):
    main()
