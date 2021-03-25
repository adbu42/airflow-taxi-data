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

# ---------------------------- download files, put them into hdfs and merge them --------------------------------------

month_numbers = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
create_raw_hdfs_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_raw',
    directory='/user/hadoop/NYCTaxiRAW',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_final_hdfs_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_final',
    directory='/user/hadoop/NYCTaxiFinal',
    hdfs_conn_id='hdfs',
    dag=dag,
)

for month_number in month_numbers:
    download_taxi_data = HttpDownloadOperator(
       task_id='download_taxi_' + month_number,
       download_uri='https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-' + month_number + '.csv',
       save_to='/home/airflow/NYCTaxi/taxi_2019_' + month_number + '.csv',
       dag=dag,
    )

    hdfs_put_taxi_data = HdfsPutFileOperator(
        task_id='upload_taxi_' + month_number + '_to_hdfs',
        local_file='/home/airflow/NYCTaxi/taxi_2019_' + month_number + '.csv',
        remote_file='/user/hadoop/NYCTaxiRAW/yellow_tripdata_2019-' + month_number + '.csv',
        hdfs_conn_id='hdfs',
        dag=dag,
    )

    clear_local_import_dir >> download_taxi_data >> create_raw_hdfs_dir >> hdfs_put_taxi_data >> create_final_hdfs_dir

# ---------------------------------- clean data and move it to hdfs_final ------------------------------------

pyspark_merge_taxi_csvs = SparkSubmitOperator(
    task_id='pyspark_merge_taxi_CSVs',
    conn_id='spark',
    application='/home/airflow/airflow/python/merge_taxi_CSVs.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='merge_taxi_CSVs',
    verbose=True,
    dag=dag
)


# ------------------------------ connections ----------------------------------------------------

create_local_import_dir >> clear_local_import_dir
create_final_hdfs_dir >> pyspark_merge_taxi_csvs