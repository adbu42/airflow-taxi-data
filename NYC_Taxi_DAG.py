# -*- coding: utf-8 -*-

"""
Title: DAG for NYC Taxi Variables
Author: Adrian Buchwald
Description: Download and produce KPIs for NYC Taxi Data
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}

dag = DAG('NYCTaxi', default_args=args, description='Create KPIs from NYC Taxi Data',
          schedule_interval='0 12 * * *',
          start_date=datetime(2021, 3, 20), catchup=False, max_active_runs=1)

# ------------------------- create import dir -------------------------------------

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='NYCTaxi',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/NYCTaxi',
    pattern='*',
    dag=dag,
)

# ---------------------------- download and merge files ------------------------------------------

month_numbers = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
dummy_operator = DummyOperator()

for month_number in month_numbers:
    download_taxi_data = HttpDownloadOperator(
       task_id='download_taxi_01',
       download_uri='https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-' + month_number + '.csv',
       save_to='/home/airflow/imdb/taxi_2019_' + month_number + '.csv',
       dag=dag,
    )

    clear_local_import_dir >> download_taxi_data >> dummy_operator


# ------------------------------ connections ----------------------------------------------------

create_local_import_dir >> clear_local_import_dir
