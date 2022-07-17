from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from utils.default_args import default_args


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


with DAG(
    'create_tables_dag',
    default_args=default_args,
    description='Create the datamart tables in Redshift with Airflow',
    schedule_interval=None
) as _:
    create_tables = PostgresOperator(
        task_id="Create_tables",
        postgres_conn_id="redshift_connection",
        sql="sql/create_tables.sql"
    )
