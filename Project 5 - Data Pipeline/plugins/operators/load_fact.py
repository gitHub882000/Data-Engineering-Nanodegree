from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 fact_table_name,
                 sql,
                 redshift_conn_id,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.fact_table_name = fact_table_name
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Inserting data to table {self.fact_table_name}.")
        redshift_hook.run("INSERT INTO {} {};".format(
            self.fact_table_name,
            self.sql
        ))
