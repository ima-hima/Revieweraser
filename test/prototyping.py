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


def using_databricks(input_tuple, sc, interim_profiling):
    ''' Import file from S3 to a dataframe using databricks, then convert to an rdd. Do this to avoid
        having a conditional in foreach loop to remove header. Also, can remove any extra columns. '''
    sqlContext = SQLContext(sc)
    overall_start_time = time()

    schema = StructType([
        StructField("marketplace",       StringType(),  True), # Last field is nullable.
        StructField("customer_id",       IntegerType(), True),
        StructField("review_id",         StringType(),  True),
        StructField("product_id",        StringType(),  True),
        StructField("product_parent",    IntegerType(), True),
        StructField("product_title",     StringType(),  True),
        StructField("product_category",  StringType(),  True),
        StructField("star_rating",       IntegerType(), True),
        StructField("helpful_votes",     IntegerType(), True),
        StructField("total_votes",       IntegerType(), True),
        StructField("vine",              StringType(),  True),
        StructField("verified_purchase", StringType(),  True),
        StructField("review_headline",   StringType(),  True),
        StructField("review_body",       StringType(),  True),
        StructField("review_date",       StringType(),  True),
    ])

    df = sqlContext.read \
        .format('csv') \
        .options(header='true', delimiter='\t', mode='DROPMALFORMED') \
        .schema(schema) \
        .load('s3a://eric-ford-insight-19/original/' + input_tuple[0]) \
        .createOrReplaceTempView("table1") # This is creating a temporary sql-like table that I can query against.

    rdd = sqlContext.sql("SELECT customer_id, star_rating, review_body FROM table1").rdd
    process_data(rdd, "Using databricks", input_tuple[1], overall_start_time, interim_profiling)


def process_data(inputRDD, which_method, size, overall_start_time, interim_profiling):
    ''' Accept an rdd as input. Do all necessary actions and transforms on rdd then insert into DB. '''

    print('\n\n**' + which_method + '**')
    print('\nFile size:', size)

    host, passwd, port, db = open('redis-pass.txt').readline().split()
    # print(host)
    # inputRDD = inputRDD.repartition(4)
    # print( inputRDD.first() )
    print_updates('read file:', overall_start_time, inputRDD.getNumPartitions())

    if interim_profiling:
        cur_start_time = time()
    # create initial key:val
    keyed_data = inputRDD.map(create_map_keys_fn)

    # Here and below, call to first() is to force evaluation for profiling output. Is that slowing
    # things down? Maybe, so put in condidional.
    if interim_profiling:
        r = keyed_data.first() # this to force lazy eval for timing
        print_updates('create original map:', cur_start_time, keyed_data.getNumPartitions())

    # keyed_data = keyed_data.repartition(9)
    if interim_profiling:
        cur_start_time = time()

    # Determine how many reviews each reviewer has written.
    keyed_for_counts = inputRDD.map(map_counts_fn)
    keyed_for_counts = keyed_for_counts.repartition(5)    # repartitioning here speeds things up significantly
    counts           = keyed_for_counts.reduceByKey(count_keys)
    if interim_profiling:
        r = counts.first() # this to force lazy eval for timing
        print_updates('count keys:', cur_start_time, counts.getNumPartitions())

    # Count total stars and total word count for each user, to be used eventually for sum_review_values.
    # keyed_data = keyed_data.repartition(4)
    if interim_profiling:
        cur_start_time = time()
    summed_review_values = keyed_data.reduceByKey(sum_review_values)
    if interim_profiling:
        r = summed_review_values.first()
        print_updates('average', cur_start_time, summed_review_values.getNumPartitions())

    # Now do join between per user counts and the number of reviews a user has written, so we can add to DB. After this the data should be in the form
    # (user_id, num_reviews, total_stars, total_words)
    if interim_profiling:
        cur_start_time = time()
    final = counts.join(summed_review_values).map(concat_fn)
    if interim_profiling:
        r = final.first()
        print_updates('join counts:', cur_start_time, final.getNumPartitions())


    # final = final.repartition(4)
    cur_start_time = time()
    final.foreachPartition( lambda x: redis_insert(x, host, passwd, port, db) )
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


def without_databricks(input_tuple, sc, interim_profiling):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB. '''
    overall_start_time = time()

    print('\n\n**without databricks**')
    print('\nFile size:', input_tuple[1])

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
    keyed_for_counts = keyed_for_counts.repartition(5)    # repartitioning here speeds things up significantly
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






# I haven't had a chance to actually try this yet.
# Consider moving some of this out to helper fns. Certainly do it if using_tsvs() comes back online.
def using_parquet(input_tuple):
    overall_start_time = time()

    print('\nFile size:', input_tuple[1])
    cur_start_time = time()
    sql_sc = SQLContext(sc)
    # datafile = (sc.read.schema(schema)
    #                    .option("header", "true")
    #                    .option("mode", "DROPMALFORMED")
    #                    .csv(s3n://eric-ford-insight-19/original/' + input_tuple[0])')
    dataFile = sc.textFile('s3a://eric-ford-insight-19/original/' + input_tuple[0]) # Don't forget it's s3a, not s3.
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
    sum_review_values         = keyed_data.reduceByKey(sum_review_values)
    final             = counts.join(sum_review_values).map(concat_fn)
    r = final.first()
    print('Time to join counts:        ', round(time() - cur_start_time, 2), 'secs.')
    print('Number of partitions:       ', final.getNumPartitions())
    cur_start_time = time()
    # final.foreachPartition( redis_insert )
    print('time to write to Redis:     ', round(time() - cur_start_time,2), 'secs.')

    print('\nTotal time:                 ', round(time() - overall_start_time,2), 'secs.' )

    print()
    headers = ['user:', 'number of reviews:', 'average star rating:', 'average review length:']
    for item in final.take(10):
        print( '{} {:<12} {} {:<2}   {} {:>}    {} {:>5.2f}'.format( headers[0], item[0],
                                                                 headers[1], item[1],
                                                                 headers[2], round(item[2]/item[1], 2),
                                                                 headers[3], round(item[3]/item[1], 2)
                                                               )
             )
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
    redis_db = redis.StrictRedis(host=host, password=passwd, port=int(port), db=int(db))
    for tup in iter:
        if redis_db.exists(tup[0]):
            # redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )
            redis_db.hincrby(tup[0], 'num',   tup[1])
            redis_db.hincrby(tup[0], 'stars', tup[2])
            redis_db.hincrby(tup[0], 'words', tup[3])
        else:
            redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )

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
                            ('amazon_reviews_us_Digital_Software_v1_00.tsv.gz',       '18MB'),
                            ('amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz',    '184MB'),
                            # ('amazon_reviews_us_Apparel_v1_00.tsv.gz',                '620MB'),
                            # ('amazon_reviews_us_Books_v1_02.tsv.gz',                  '1.2GB'),
                            # ('amazon_reviews_us_Wireless_v1_00.tsv.gz',               '1.6GB'),
                            # ('amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz', '2.5GB'),
                          ]:
        # using_databricks(filename_tuple, sc, False)
        without_databricks(filename_tuple, sc, True)

if(__name__ == "__main__"):
    main()



































########################### DEFUNCT CODE ##############################


# Was going to use pandas to try to deal with the header issues, but the pandas lib on my AWS instances is broken.
# After I get parquet working, if there's time I'll attempt this again. I'd like to do profiling on both.
# Maybe use conda? Remember that I have to install it on all workers: bash script?
def using_csv_rdd(input_tuple):
    ''' Read in a tsv from S3 using csv to eliminate header file. '''

    # this code adapted from O’Reilly Learning Spark
    def loadRecords(filename):
        """ Load all the records in a given file, throw away the first row. """
        reader = csv.reader(filename, delimiter='\t', newline='')
        return next(reader, None) # this is ***extremely** fragile

    s3 = boto3.resource('s3')
    overall_start_time = time()

    print('\nFile size:', input_tuple[1])
    cur_start_time = time()


    # pandas//eric-ford-insight-19/original/' + input_tuple[0])')
    dataFile = sc.textFile('s3a://eric-ford-insight-19/original/' + input_tuple[0]).map(loadRecords)
    print( dataFile.first() )
    print_updates('read file:', cur_start_time, dataFile.getNumPartitions())

    exit(1)
    cur_start_time = time()
    # create initial key:val After this we'll have tuples (user_id, [star_rating, word_count])
    keyed_data = dataFile.map(create_map_keys_fn)
    # count appearances of each key in `keyed_data`
    # r = keyed_data.first()
    print_updates('create original map:', cur_start_time, dataFile.getNumPartitions())

    cur_start_time = time()
    # keyed_for_counts = dataFile.map(map_counts_fn)
    keyed_for_counts = dataFile.map(map_counts_fn)
    # get count of each user's reviews
    counts = keyed_for_counts.reduceByKey(count_keys)
    # r = counts.first()
    print_updates('count keys:', cur_start_time, dataFile.getNumPartitions())

    cur_start_time = time()
    sum_review_values         = keyed_data.reduceByKey(sum_review_values)
    final            = counts.join(sum_review_values).map(concat_fn)
    # r = final.first()
    print_updates('join counts:', cur_start_time, dataFile.getNumPartitions())

    cur_start_time = time()
    final.foreachPartition( redis_insert )
    print_updates('write to Redis:', cur_start_time, dataFile.getNumPartitions())

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
