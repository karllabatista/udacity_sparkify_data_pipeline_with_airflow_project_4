from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'

    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credential_id="",
                table ="",
                s3_bucket="",
                s3_key="",
                s3_json_path="",
                delimeter=",",
                ignore_headers=1,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key = s3_key
        self.s3_json_path = s3_json_path
        self.delimeter=delimeter
        self.ignore_headers=ignore_headers



    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        metaStoreBackend = MetastoreBackend()
        aws_connection= metaStoreBackend.get_connection(self.aws_credential_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Connecting to redshift .. ")
        conn = redshift.get_conn()
        cursor= conn.cursor()
        
        # Test connection
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        if result:
            self.log.info('Connection to Redshift successfully established')

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift")
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            s3_json_path = "s3://{}/{}".format(self.s3_bucket, self.s3_json_path)
            print("--->",s3_path)
            print("-->", s3_json_path)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            s3_json_path
            )
            redshift.run(formatted_sql)
            
        else:
            self.log.warning('Fail to established connection with Redshift')

        
        self.log.info('StageToRedshiftOperator concluído')    

