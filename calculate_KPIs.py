import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc, lit, unix_timestamp, when, hour, col, month, year


def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(
        description='Some Basic Spark Job doing some stuff on IMDb data stored within HDFS.')
    return parser.parse_args()


if __name__ == '__main__':
    """
    Main Function
    """

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    taxi_data = spark.read.csv("data/example.csv")