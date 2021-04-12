# airflow-taxi-data
This project analyses NYC taxi data with aiflow, hadoop and pyspark.

calculate_KPIs is a pyspark skript that claculates the KPIs: 
average trip duration, 
the average total amount, 
the average trip amount, 
the average trip distance, 
the average tip amount, 
the average passenger count, 
the usage share of payment type and the usage share of the time slots


merge_taxi_CSVs is a pyspark skript that merges all taxi-files into one partitioned taxi-file. It also adds the colums TripDuration and TimeSlot.

NYC_taxi_DAG is the complete airflow-DAG for the NYC taxi-files.

NYC_taxi_KPIs contains the taxi-KPIs.
 