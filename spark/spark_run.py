''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

# import com.datastax.spark.connector._

from csv         import DictReader
from pyspark     import SparkConf, SparkContext
# from pyspark.sql import SQLContext


def main(input_filename = '../test/test_input.tsv'):
    spark_conf = SparkConf().setAppName("Batch processing") #("spark.cores.max", "1")
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")

    dataFile = sc.textFile(input_filename)
    header = dataFile.first()

    keyed_data       = dataFile.filter(lambda line: line != header).map(map_fn)
    keyed_for_counts = dataFile.filter(lambda line: line != header).map(map_counts_fn)
    counts           = keyed_for_counts.reduceByKey(count_keys)
    averages         = keyed_data.reduceByKey(average_reviews)
    final            = counts.join(averages).map(concat_fn)
    final.saveAsTextFile("spark_output")

    print final.take(10)
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

def map_fn(line):
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])


def map_counts_fn(line):
    line = line.split('\t') # there's got to be a faster way
    return (line[1], 1)

def concat_fn(line):
    return (line[0], line[1][0], line[1][1][0], line[1][1][1])


def average_reviews(accum, input_list):
    return [accum[0] + input_list[0], accum[1] + input_list[1]]


def count_keys(accum, input_value):
    return accum + 1



if(__name__ == "__main__"):
    main()


