from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            count = redshift.get_records(f'SELECT COUNT(*) FROM {table}') 
   
            if count[0][0] == 0:
                self.log.error(f"-- QUALITY CHECK -- {table} has no rows -- ERROR --")
                raise ValueError(f"-- QUALITY CHECK -- {table} has no rows -- ERROR --")
            else:
                self.log.error(f"-- QUALITY CHECK -- {table} has rows -- {count[0][0]} --")
            