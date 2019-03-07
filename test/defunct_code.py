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







def without_join(input_tuple, sc, interim_profiling):
    ''' Import a file from S3 directly to an rdd. Process that rdd and write out to Redis DB.
        Skip the count and join steps and aggregate directly into Redis. This is about 5x slower than
        doing counts and joins in Spark. '''
    overall_start_time = time()

    print('\n\n**without join**')
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

    # if interim_profiling:
    #     r = keyed_data.first() # this to force lazy eval for timing
    #     print_updates('create original map:', cur_start_time, keyed_data.getNumPartitions())

    # # Count and join would be here, but now they're not.

    # if interim_profiling:
    #     r = counts.first() # this to force lazy eval for timing
    #     print_updates('count keys:', cur_start_time, keyed_for_counts.getNumPartitions())

    # # Count total stars and total word count for each user, to be used eventually for sum_review_values.
    # if interim_profiling:
    #     cur_start_time = time()
    # per_review_totals = keyed_data.reduceByKey(sum_review_values)
    # if interim_profiling:
    #     r = per_review_totals.first()
    #     print_updates('total counts per review:', cur_start_time, per_review_totals.getNumPartitions())

    # # Now do join between per user counts and the number of reviews a user has written, so we can add to DB. After this the data should be in the form
    # # (user_id, num_reviews, total_stars, total_words)
    # if interim_profiling:
    #     cur_start_time = time()
    # final = counts.join(per_review_totals).map(concat_fn)
    # if interim_profiling:
    #     r = final.first() # this to force lazy eval for timing
    #     print_updates('join:', cur_start_time, counts.getNumPartitions())

    # cur_start_time = time()
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
    keyed_for_counts = keyed_for_counts.repartition(10)    # repartitioning here speeds things up significantly
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
