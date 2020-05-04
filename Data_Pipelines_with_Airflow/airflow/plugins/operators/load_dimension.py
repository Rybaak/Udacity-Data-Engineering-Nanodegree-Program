from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 sql = '',
                 append = False,
                 table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql
        self.append = append
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append is False:
            self.log.info(f'Clearing {self.table}.')
            self.log.info("DELETE FROM {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f"Loading dimensions - {self.table}")
        if self.sql is not None and self.sql != '':
            redshift.run(self.sql)
