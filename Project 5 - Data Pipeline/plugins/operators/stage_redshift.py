from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ('data_key', 'jsonpath_key')
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 staging_table_name,
                 data_key,
                 jsonpath_key,
                 aws_conn_id,
                 redshift_conn_id,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.staging_table_name = staging_table_name
        self.data_key = data_key
        self.jsonpath_key = jsonpath_key
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        aws_credentials = S3Hook(aws_conn_id=self.aws_conn_id).get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data in table {self.staging_table_name}.")
        redshift_hook.run("TRUNCATE {};".format(self.staging_table_name))
        
        self.log.info(F"Copying data from {self.data_key} to table {self.staging_table_name}.")
        
        redshift_hook.run(
            StageToRedshiftOperator.copy_sql.format(
                self.staging_table_name,
                self.data_key,
                aws_credentials.access_key,
                aws_credentials.secret_key,
                self.jsonpath_key
            )
        )