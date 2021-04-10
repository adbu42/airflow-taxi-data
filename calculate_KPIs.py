import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc, lit, unix_timestamp, when, hour, col, month, year, avg
from pyspark.sql import SQLContext
import pandas as pd
import xlsxwriter

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

    taxi_data = spark.read.parquet("/user/hadoop/NYCTaxiFinal")

    avg_trip_duration = taxi_data.agg(avg('TripDuration')).toPandas()

    avg_trip_distance = taxi_data.agg(avg('trip_distance')).toPandas()

    avg_total_amount = taxi_data.agg(avg('total_amount')).toPandas()

    avg_tip_amount = taxi_data.agg(avg('tip_amount')).toPandas()

    avg_passenger_count = taxi_data.agg(avg('passenger_count')).toPandas()

    usage_share_payment_type = taxi_data.groupBy('payment_type').count().orderBy(col('count').desc()).toPandas()
    total_payment_type_share = usage_share_payment_type['count'].sum()
    usage_share_payment_type['usage_share'] = usage_share_payment_type['count'] / total_payment_type_share

    usage_share_time_slot = taxi_data.groupBy('TimeSlot').count().orderBy(col('count').desc()).toPandas()
    total_time_slot_share = usage_share_time_slot['count'].sum()
    usage_share_time_slot['usage_share'] = usage_share_time_slot['count'] / total_time_slot_share

    writer = pd.ExcelWriter('/home/airflow/airflow/NYC_taxi_KPIs.xlsx', engine='xlsxwriter')
    workbook = writer.book
    worksheet = workbook.add_worksheet('KPIs')
    writer.sheets['KPIs'] = worksheet
    avg_trip_duration.to_excel(writer, sheet_name='KPIs', startrow=0, startcol=0)
    avg_total_amount.to_excel(writer, sheet_name='KPIs', startrow=5, startcol=0)
    avg_trip_distance.to_excel(writer, sheet_name='KPIs', startrow=10, startcol=0)
    avg_tip_amount.to_excel(writer, sheet_name='KPIs', startrow=15, startcol=0)
    avg_passenger_count.to_excel(writer, sheet_name='KPIs', startrow=20, startcol=0)
    usage_share_payment_type.to_excel(writer, sheet_name='KPIs', startrow=0, startcol=10)
    usage_share_time_slot.to_excel(writer, sheet_name='KPIs', startrow=10, startcol=10)
    workbook.close()
