""" In order to decide whether parquet files will actually be faster, I need them. Although Amazon says you can
    get parquet files from them, I couldn't get it to work, so I'm making my own. """

from pyspark    import SparkConf, SparkContext
from pyspark.sql import SparkSession


def conversion(filename_tuple):
        print('starting', filename_tuple[0])

        rdd = sc.textFile('s3a://eric-ford-insight-19/original/' + filename_tuple[0]) # Don't forget it's s3a, not s3.

        header     = rdd.first()
        # create initial key:val
        keyed_data = rdd.filter(lambda line: line != header).map(create_map_keys_fn)

        df = spark.createDataFrame(keyed_data)

        df.write.mode("overwrite").parquet("s3a://eric-ford-insight-19/parquet_files/" + filename_tuple[0][:-6] + 'parquet')
        print('done with', filename_tuple[0])

def create_map_keys_fn(line):
    ''' Return a tuple with key : val = user_id : [star rating, number of words]. '''
    line = line.split('\t')
    return (line[1], [int(line[7]), line[13].count(' ') + 1])


# Start this out here because I can only use one of these.
spark_conf = SparkConf().setAppName("To parquet") #("spark.cores.max", "1")
sc         = SparkContext(conf=spark_conf)
spark      = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()

for filename_tuple in [ ('amazon_reviews_us_Digital_Software_v1_00.tsv.gz',       '18MB'),
                        ('amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz',    '184MB'),
                        ('amazon_reviews_us_Apparel_v1_00.tsv.gz',                '620MB'),
                        ('amazon_reviews_us_Books_v1_02.tsv.gz',                  '1.2GB'),
                        ('amazon_reviews_us_Wireless_v1_00.tsv.gz',               '1.6GB'),
                        ('amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz', '2.5GB'),
                      ]:
    conversion(filename_tuple)
