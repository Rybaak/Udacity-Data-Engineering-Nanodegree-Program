from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 4, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 's3://udacity-dend',
    s3_key = 'log_data',
    log_jsonpath = 's3://udacity-dend/log_json_path.json',
    timeformat = 'epochmillisecs'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = 's3://udacity-dend',
    s3_key = 'song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    sql = SqlQueries.songplay_table_insert,
    append = False ,
    table = 'songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    sql = SqlQueries.user_table_insert,
    append = False ,
    table = 'users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    sql = SqlQueries.song_table_insert,
    append = False ,
    table = 'songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    sql = SqlQueries.artist_table_insert,
    append = False ,
    table = 'artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    sql = SqlQueries.time_table_insert,
    append = False ,
    table = 'time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ['artists', 'songplays', 'songs', 'time', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator