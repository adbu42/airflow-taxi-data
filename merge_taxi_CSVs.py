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

    # Read Taxi Data from HDFS
    month_numbers = ['01', '02', '03']

    NYC_taxi_data = []
    timeFmt = "yyyy-MM-dd HH:mm:ss"
    timeDiff = (unix_timestamp(col("tpep_dropoff_datetime"), format=timeFmt)
                - unix_timestamp(col("tpep_pickup_datetime"), format=timeFmt))
    getTimeSlot = (when(hour(col("tpep_pickup_datetime")) <= 6, '0to6')
                   .when((6 < hour(col("tpep_pickup_datetime"))) & (hour(col("tpep_pickup_datetime")) <= 12), '6to12')
                   .when((12 < hour(col("tpep_pickup_datetime"))) & (hour(col("tpep_pickup_datetime")) <= 18), '12to18')
                   .when((18 < hour(col("tpep_pickup_datetime"))) & (hour(col("tpep_pickup_datetime")) <= 24), '18to24')
                   .otherwise('unknown'))

    for month_number in month_numbers:
        taxi_dataframe = spark.read.format('csv').options(header='true', delimiter=',', nullValue='null',
                                                          inferschema='true').load(
            '/user/hadoop/NYCTaxiRAW/yellow_tripdata_2019-' + month_number + '.csv')
        taxi_dataframe = taxi_dataframe.withColumn('Year', year(col("tpep_pickup_datetime")))
        taxi_dataframe = taxi_dataframe.withColumn('Month', month(col("tpep_pickup_datetime")))
        taxi_dataframe = taxi_dataframe.withColumn('TripDuration', timeDiff)
        taxi_dataframe = taxi_dataframe.withColumn('TimeSlot', getTimeSlot)
        if month_number == '01':
            NYC_taxi_data = taxi_dataframe
        else:
            NYC_taxi_data = NYC_taxi_data.union(taxi_dataframe)

    NYC_taxi_data.repartition('Year', 'Month').write.format("parquet").mode("overwrite").partitionBy('Year', 'Month') \
        .save('/user/hadoop/NYCTaxiFinal')
