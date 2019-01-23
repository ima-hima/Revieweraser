''' Run Spark.
    This is expecting a .tsv file in the next directory up.
    Current criteria for removal: two or more reviews that are 5 stars and have five or fewer words. '''

from csv             import DictReader
from pyspark.conf    import SparkConf
from pyspark.context import SparkContext


def main(input_filename = '../test/test_input.tsv'):
    # spark_conf = SparkConf().setAppName("Batch processing").set("spark.cores.max", "1")

    # users is a dictionary of lists: user_id : [total reviews, number of stars in this review, length of this review, allow?]
    users = dict()
    with open(input_filename) as input_stream:
        reader = DictReader(input_stream, delimiter='\t')
        for row in reader:
            print( 'customer id:', row['customer_id'] )
            try:
                users[row['customer_id']][0] += 1 # It'll fail early, so next work will only run once, here or
                                                  # in except.
                users[row['customer_id']][1] += int(row['star_rating'])
                users[row['customer_id']][2] += row['review_body'].count(' ') # number of words - 1
            except:
                users[row['customer_id']] = [1, int(row['star_rating']), row['review_body'].count(' '), True]

    multiple = 0 # Will tell me how many will be eliminated.
                 # See criteria in module comment.
    for user in users:
        average_rating = users[user][1] / users[user][0] # trying to save heap accesses
        if users[user][0] > 1 and (average_rating == 5 or average_rating == 1) and users[user][2] / users[user][0] < 6:
            users[user][3] = False
            multiple += 1

    print( 'total users:', len(users) )
    print( 'total eliminations:', multiple )

def cassandra_output():

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print(colored("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()


