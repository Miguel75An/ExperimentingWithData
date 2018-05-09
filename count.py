import sys
from pyspark import SparkContext


def extractRow(index, lines):
    import csv
    if index == 0:
        lines.next()

    reader = csv.reader(lines)
    for row in reader:
        yield row

if __name__ == '__main__':
    sc = SparkContext()

    reviews_rdd = sc.textFile("/user/ipalong00/yelp_reviews/yelp_review.csv", use_unicode=False)
    reviews_rdd_new = reviews_rdd.mapPartitionsWithIndex(extractRow).cache()

    # Create the schema necessary for the creation of a DataFrame
    from pyspark.sql.types import *

    schema = StructType([
        StructField("review_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("business_id", StringType(), False),
        StructField("stars", StringType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("useful", StringType(), True),
        StructField("funny", StringType(), True),
        StructField("cool", StringType(), True)])

    # Create a dataframe using the RDD and the previously declared schema
    reviews_df = spark.createDataFrame(reviews_rdd_new, schema)
    reviews_df.printSchema()

    # Cast all columns to the appropriate datatype
    reviews_df = reviews_df.select(reviews_df['review_id'],
                                   reviews_df['user_id'],
                                   reviews_df['business_id'],
                                   reviews_df['stars'].cast(IntegerType()),
                                   reviews_df['date'].cast(DateType()),
                                   reviews_df['text'],
                                   reviews_df['useful'].cast(IntegerType()),
                                   reviews_df['funny'].cast(IntegerType()),
                                   reviews_df['cool'].cast(IntegerType()),
                                   ).cache()
    print('HEEEEEYYY')
    print(reviews_df.count())


