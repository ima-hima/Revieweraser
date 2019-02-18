''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from pyspark    import SparkConf, SparkContext
from subprocess import check_output



def main():
    ''' Loop over input files from S3. '''

    # first set up spark context
    spark_conf = SparkConf().setAppName("Batch processing") #("spark.cores.max", "1")
    sc         = SparkContext(conf=spark_conf)

    # Call aws result is a space delimited string file names and metadata
    input_list = ['aws', 's3', 'ls', 's3://eric-ford-insight-19/original/']
    file_list  = check_output(input_list)
    file_list  = str(files, encoding='utf-8').split('\n')

    for f in file_list[1:-1]: # The first item is '0' and the last one is empty
        process_file(file, sc)


def process_file(input_filename, sc):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB. '''
    # I moved the password, etc. into another file that won't go on github.
    host, passwd, port, db = open('redis-pass.txt').readline().split()
    originalRDD = sc.textFile('s3a://eric-ford-insight-19/original/' + input_filename) # Don't forget it's s3a, not s3.

    # Storing the header because I'm going to filter on it later.
    header = originalRDD.first()

    # create initial key:val. After this rows will be (user_id, [star rating, number of words])
    keyed_data = originalRDD.filter(lambda line: line != header).map(create_map_keys_idx_fn)

    # Determine how many reviews each reviewer has written.
    keyed_for_counts = originalRDD.filter(lambda line: line != header).map(map_counts_rdd_fn)
    counts           = keyed_for_counts.reduceByKey(count_keys)

    # Count total stars and total word count for each user, to be used eventually for sum_review_values.
    per_review_totals = keyed_data.reduceByKey(sum_review_values)

    # Now do join between per user counts and the number of reviews a user has written, so we can add to DB. After this the data should be in the form
    # (user_id, num_reviews, total_stars, total_words)
    final = counts.join(per_review_totals).map(concat_fn)

    final.foreachPartition( lambda x: redis_insert(x, host, passwd, port, db) )


def redis_insert(iter, host, passwd, port, db):
    ''' Insert tuple (rdd) into Redis. Tuple is of form (key, [int, int, int]), where the ints are
        number of reviews, total star rating, total number of words, respectively. '''
    import redis
    redis_db = redis.StrictRedis(host=host, password=passwd, port=int(port), db=int(db))
    for tup in iter:
        if redis_db.exists(tup[0]):
            redis_db.hincrby(tup[0], 'num',   tup[1])
            redis_db.hincrby(tup[0], 'stars', tup[2])
            redis_db.hincrby(tup[0], 'words', tup[3])
        else:
            redis_db.hmset(tup[0], {'num': tup[1], 'stars': tup[2], 'words': tup[3]} )


def create_map_keys_idx_fn(line):
    ''' Same as create_map_keys_fn, but using indices rather than keys. '''
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])


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


def create_map_keys_fn(line):
    ''' Return a tuple with key : val = user_id : [star rating, number of words]. '''
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])




if(__name__ == "__main__"):
    main()


