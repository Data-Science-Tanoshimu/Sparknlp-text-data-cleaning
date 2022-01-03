from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import explode, monotonically_increasing_id, to_date

import sparknlp
from sparknlp.base import DocumentAssembler, TokenAssembler
from sparknlp.annotator import Tokenizer, Normalizer
from pyspark.ml import Pipeline

from pyspark.sql.types import StructType,StructField, StringType, DateType

# Start sparknlp
sparknlp.start()
# Start spark
spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName("test") \
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .getOrCreate()
# Temp bucket for bigquery
spark.conf.set('temporaryGcsBucket', 'clean-data-temp')

# Input bucket, filename, schema
input_bucket = 'gs://raw-data-cleaner/'
filename = 'reddit_thinkpad.csv'
schema = StructType(
    [
        StructField('title', StringType(), False),
        StructField('selftext', StringType(), False),
        StructField('sentiment', StringType(), False),
        StructField('date', DateType(), False),
    ]
)

# Read csv
def read_csv(bucket_name, filename, schema):
    try:
        return spark.read.options(header='True', inferSchema='True', delimiter=',', schema=schema).csv(bucket_name + filename)
    except AnalysisException:
        return None

# make pipeline to clean data
def clean_data(df, col_name):
    name_str = 'cleaned_'
    documentAssembler = DocumentAssembler() \
        .setInputCol(col_name) \
        .setOutputCol('document')
    tokenizer = Tokenizer() \
        .setInputCols(['document']) \
        .setOutputCol('token')
    # note normalizer defaults to changing all words to lowercase.
    # setCleanupPatterns delete words matching with the regex
    normalizer = Normalizer() \
        .setInputCols(['token']) \
        .setOutputCol('normalized') \
        .setLowercase(True)\
        .setCleanupPatterns(["http(s)?://([\w-]+\.)+[\w-]+(/[\w- ./?%&=~]*)?", "[^a-zA-Z0-9]", ","])    
    tokenassembler = TokenAssembler()\
        .setInputCols(["document", "normalized"]) \
        .setOutputCol(name_str + col_name)

    pipeline = Pipeline() \
        .setStages([
            documentAssembler,
            tokenizer,
            normalizer,
            tokenassembler
        ])

    return pipeline.fit(df).transform(df), name_str + col_name

# you have to extract cleaned data from the data coming from the pipeline
def df_from_cleaned_data(cleaned_data, col_name):
    cleaned_df = cleaned_data.select(explode(col_name + '.result').alias(col_name))

    return cleaned_df.withColumn('id', monotonically_increasing_id())
  

# Save the data into BigQuery
def save_bigquery(df, table_name):
    df.write\
        .format('bigquery') \
        .mode('overwrite') \
        .option('table', table_name) \
        .save()


def main():
    # Read csv
    df = read_csv(input_bucket, filename, schema)
    df = df.withColumn('id', monotonically_increasing_id())

    # Data cleaning
    cleaned_title_data, cleaned_title_col = clean_data(df, 'title')
    cleaned_selftext_data, cleaned_selftext_col = clean_data(df, 'selftext')

    # Extract cleaned data
    cleaned_title_df = df_from_cleaned_data(cleaned_title_data, cleaned_title_col)
    cleaned_selftext_df = df_from_cleaned_data(cleaned_selftext_data, cleaned_selftext_col)

    # Join dataframes
    df = df.join(cleaned_title_df, df.id == cleaned_title_df.id, "left").drop(cleaned_title_df.id)
    df = df.join(cleaned_selftext_df, df.id == cleaned_selftext_df.id, "left").drop(cleaned_selftext_df.id)

    # Keep cleaned data and rename them
    df = df.drop('title', 'selftext')
    df = df.withColumnRenamed('cleaned_title', 'title').withColumnRenamed("cleaned_selftext", 'selftext')

    # remove rows containing empty, 'removed', and null 
    df = df.replace(['', 'removed'], None, ['title', 'selftext'])
    df = df.filter(df.title.isNotNull() & df.selftext.isNotNull())

    # Insert into bigquery
    to_bigquery = df.select('title', 'selftext', 'sentiment', to_date('date', 'yyyy-MM-dd').alias('date'))
    to_bigquery.createOrReplaceTempView('dataset')
    save_bigquery(to_bigquery, 'reddit_sentiment.post')

    print('Done!')


if __name__ == "__main__":
    main()