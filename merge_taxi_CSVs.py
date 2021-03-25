import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc, lit, unix_timestamp, when, hour

if __name__ == '__main__':
    """
    Main Function
    """

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read Taxi Data from HDFS
    month_numbers = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    NYC_taxi_data = []
    timeFmt = "yyyy-MM-dd HH:mm:ss"
    timeDiff = (unix_timestamp('tpep_dropoff_datetime', format=timeFmt)
                - unix_timestamp('tpep_pickup_datetime', format=timeFmt))
    getTimeSlot = (when(hour(unix_timestamp('tpep_pickup_datetime', format=timeFmt)) <= 6, '0to6')
                   .when(6 < hour(unix_timestamp('tpep_pickup_datetime', format=timeFmt)) <= 12, '6to12')
                   .when(12 < hour(unix_timestamp('tpep_pickup_datetime', format=timeFmt)) <= 18, '12to18')
                   .when(18 < hour(unix_timestamp('tpep_pickup_datetime', format=timeFmt)) <= 24, '18to24')
                   .otherwise('unknown'))

    for month_number in month_numbers:
        taxi_dataframe = spark.read.format('csv') \
            .options(header='true', delimiter='\t', nullValue='null', inferschema='true') \
            .load('/user/hadoop/NYCTaxiRAW/yellow_tripdata_2019-' + month_number + '.csv')
        taxi_dataframe = taxi_dataframe.withColumn('Year', lit(2019))
        taxi_dataframe = taxi_dataframe.withColumn('Month', lit(int(month_number)))
        taxi_dataframe = taxi_dataframe.withColumn('TripDuration', timeDiff)
        taxi_dataframe = taxi_dataframe.withColumn('TimeSlot', getTimeSlot)
        if month_number == '01':
            NYC_taxi_data = taxi_dataframe
        else:
            NYC_taxi_data = NYC_taxi_data.union(taxi_dataframe)

    NYC_taxi_data.repartition('Month').write.format("parquet").mode("overwrite").partitionBy('Month') \
        .save('/user/hadoop/NYCTaxiFinal')
