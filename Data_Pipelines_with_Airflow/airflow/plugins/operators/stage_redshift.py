from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 log_jsonpath = 'auto',
                 timeformat = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_jsonpath = log_jsonpath
        self.timeformat = timeformat
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        timeformat_str = ''
        
        if self.timeformat != '':
            timeformat_str = f"timeformat as '{self.timeformat}'"
                
        self.log.info("DELETE FROM {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")   
        
        
        if self.execution_date:
            formatted_sql =     f"""
                                    COPY {self.table} FROM {self.s3_bucket}/{self.s3_key}/{self.execution_date.strftime("%Y")}/{self.execution_date.strftime("%d")}'
                                    ACCESS_KEY_ID '{credentials.access_key}'
                                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                                    json '{self.log_jsonpath}'
                                    {timeformat_str};
                                """
        else:
            formatted_sql =     f"""
                                    COPY {self.table} FROM '{self.s3_bucket}/{self.s3_key}'
                                    ACCESS_KEY_ID '{credentials.access_key}'
                                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                                    json '{self.log_jsonpath}'
                                    {timeformat_str};
                                """
        redshift.run(formatted_sql)

