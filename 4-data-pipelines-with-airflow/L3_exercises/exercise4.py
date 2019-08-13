import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)


dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

check_trips = HasRowsOperator(
    task_id='check_trips_data',
    dag=dag,
    redshift_conn_id="redshift",
    table="trips"
)

calculate_facts = FactsCalculatorOperator(
    task_id='calculate_facts',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trip_facts",
    fact_column="tripduration",
    groupby_column="bikeid",
)

copy_trips_task >> check_trips
check_trips >> calculate_facts