from pyspark.sql import SparkSession
import argparse

def main(params):
    spark = SparkSession.builder.getOrCreate()
    current_timestamp = params.date
    print(current_timestamp)
    print(spark.sparkContext)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='argument for spark jobs')

    parser.add_argument('--date', default='aaa ')
    args = parser.parse_args()
    main(args)