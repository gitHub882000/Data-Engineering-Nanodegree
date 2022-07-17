from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 dimension_table_name,
                 sql,
                 redshift_conn_id,
                 mode,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.dimension_table_name = dimension_table_name
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == 'delete-load':
            self.log.info(f"Clearing data in table {self.dimension_table_name}.")
            redshift_hook.run("TRUNCATE {};".format(self.dimension_table_name))
        elif self.mode != 'append-only':
            raise ValueError("Invalid mode, only accept \"append-only\" or \"delete-load\".")
        
        self.log.info(f"Inserting data to table {self.dimension_table_name}.")
        redshift_hook.run("INSERT INTO {} {};".format(
            self.dimension_table_name,
            self.sql
        ))
