from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_names,
                 redshift_conn_id,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Checking data in tables {self.table_names}.")
        for table_name in self.table_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name};")
            
            if not records or not records[0]:
                raise ValueError("SELECT COUNT(*) failed to execute properly.")

            if records[0][0] < 1:
                raise ValueError(f"Table {table_name} failed in data quality check with {records[0][0]} records.")

            self.log.info(f"Table {table_name} passed in data quality check with {records[0][0]} records.")
