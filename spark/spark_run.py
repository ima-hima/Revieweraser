''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from csv        import DictReader
from pyspark    import SparkConf, SparkContext
from subprocess import call, check_output
from time       import time
# from pyspark.sql import SQLContext


def main(input_filename = 'amazon_reviews_us_Books_v1_00.tsv.gz'):
    start_time = time()
    spark_conf = SparkConf().setAppName("Batch processing") #("spark.cores.max", "1")
    sc         = SparkContext(conf=spark_conf)

    # for loop is in case I decide to have multiple outputs later
    for output_file in ['spark_output']:
        if check_output(['aws', 's3', 'ls', 's3://eric-ford-insight-19/']).find(output_file)  >= 0:
            call(['aws', 's3', 'rm', 's3://eric-ford-insight-19/spark_output', '--recursive'])
        # This because aws cli fails in an ugly way if a specified file is missing. So I check for it.
        if check_output(['aws', 's3', 'ls', 's3://eric-ford-insight-19/']).find(output_file + '_$folder$') >= 0:
            call(['aws', 's3', 'rm', 's3://eric-ford-insight-19/spark_output_$folder$'])

    dataFile = sc.textFile('s3n://eric-ford-insight-19/original/' + input_filename) # Don't forget it's s3n, not s3.
    header   = dataFile.first()

    # create initial key:val
    keyed_data       = dataFile.filter(lambda line: line != header).map(create_map_keys_fn)
    # count appearances of each key in `keyed_data`
    keyed_for_counts = dataFile.filter(lambda line: line != header).map(map_counts_fn)
    # get count of each user's reviews
    counts           = keyed_for_counts.reduceByKey(count_keys)
    #
    averages         = keyed_data.reduceByKey(average_reviews)
    final            = counts.join(averages).map(concat_fn)
    final.saveAsTextFile('s3n://eric-ford-insight-19/spark_output')

    print final.take(10)
    call(['aws', 's3', 'ls', 's3://eric-ford-insight-19/'])
    print time() - start_time
    # sqlContext = SQLContext(sc)

    # df = sqlContext.read.csv(input_filename, header='true', mode="DROPMALFORMED")
    # final = df.rdd.map(map_fn).reduceByKey(reduce_fn)

    # users is a dictionary of lists: user_id : [total reviews, number of stars in this review, length of this review, allow?]
    # users = dict()

    # with open(input_filename) as input_stream:
    #     reader = DictReader(input_stream, delimiter='\t')
    #     for row in reader:
    #         print( 'customer id:', row['customer_id'] )
    #         try:
    #             users[row['customer_id']][0] += 1 # It'll fail early, so next work will only run once, here or
    #                                               # in except.
    #             users[row['customer_id']][1] += int(row['star_rating'])
    #             users[row['customer_id']][2] += row['review_body'].count(' ') # number of words - 1
    #         except:
    #             users[row['customer_id']] = [1, int(row['star_rating']), row['review_body'].count(' '), True]

    # multiple = 0 # Will tell me how many will be eliminated.
    #              # See criteria in module comment.
    # for user in users:
    #     average_rating = users[user][1] / users[user][0] # trying to save heap accesses
    #     if users[user][0] > 1 and (average_rating == 5 or average_rating == 1) and users[user][2] / users[user][0] < 6:
    #         users[user][3] = False
    #         multiple += 1

    # print( 'total users:', len(users) )
    # print( 'total eliminations:', multiple )

def create_map_keys_fn(line):
    ''' Return a tuple with key : val = user_id : [star rating, number of words]. '''
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])


def map_counts_fn(line):
    ''' Create a tuple of key : val as user_id : 1. This to be used later for counting number of times
        each keys shows up in map. '''
    line = line.split('\t') # there's got to be a faster way
    return (line[1], 1)

def concat_fn(line):
    ''' Receive a tuple of form (key, []) a Return a tuple with . '''
    return (line[0], line[1][0], line[1][1][0], line[1][1][1])


def average_reviews(accum, input_list):
    return [accum[0] + input_list[0], accum[1] + input_list[1]]


def count_keys(accum, input_value):
    ''' Increment each time it's called. '''
    return accum + 1



if(__name__ == "__main__"):
    main()


