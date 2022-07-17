from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from utils.default_args import default_args


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


with DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    end_date=datetime(2018, 11, 2),
    schedule_interval='0 0 * * *'
) as _:
    begin_execution = DummyOperator(task_id='Begin_execution')

    stage_events = StageToRedshiftOperator(
        task_id='Stage_events',
        staging_table_name='staging_events',
        data_key='{{ var.value.log_data }}/{{ execution_date.year }}/{{ execution_date.month }}/',
        jsonpath_key='{{ var.value.log_jsonpath }}',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift_connection'
    )

    stage_songs = StageToRedshiftOperator(
        task_id='Stage_songs',
        staging_table_name='staging_songs',
        data_key='{{ var.value.song_data }}/A/A/A/',
        jsonpath_key='auto',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift_connection'
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        fact_table_name='songplays',
        sql=SqlQueries.songplay_table_insert,
        redshift_conn_id='redshift_connection'
    )
    
    load_song_dim_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dimension_table_name='songs',
        sql=SqlQueries.song_table_insert,
        redshift_conn_id='redshift_connection',
        mode='delete-load'
    )

    load_user_dim_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dimension_table_name='users',
        sql=SqlQueries.user_table_insert,
        redshift_conn_id='redshift_connection',
        mode='delete-load'
    )

    load_artist_dim_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dimension_table_name='artists',
        sql=SqlQueries.artist_table_insert,
        redshift_conn_id='redshift_connection',
        mode='delete-load'
    )

    load_time_dim_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dimension_table_name='time',
        sql=SqlQueries.time_table_insert,
        redshift_conn_id='redshift_connection',
        mode='delete-load'
    )

    run_data_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        table_names=['songplays', 'songs', 'users', 'artists', 'time'],
        redshift_conn_id='redshift_connection'
    )

    stop_execution = DummyOperator(task_id='Stop_execution')
    
    begin_execution >> [stage_events, stage_songs] \
    >> load_songplays_fact_table >> [load_song_dim_table, load_user_dim_table,
                                     load_artist_dim_table, load_time_dim_table] \
    >> run_data_quality_checks >> stop_execution
