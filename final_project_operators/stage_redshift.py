from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_templated_key',)  
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'

    """
    copy_sql_2 = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as JSON 'auto'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credential_id="",
                table ="",
                s3_bucket="",
                s3_static_key="",
                s3_json_path="",
                s3_templated_key="",
                s3_json_metadatafile =False,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_static_key = s3_static_key
        self.s3_json_path = s3_json_path
        self.s3_templated_key=s3_templated_key
        self.s3_json_metadatafile=s3_json_metadatafile

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator ..')
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

            self.log.info(f"Copying {self.table} data from S3 to Redshift")
            
            # Determine which S3 key to use
            if self.s3_templated_key:
                s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_templated_key)
            else:
                s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_static_key)
                s3_json_path = "s3://{}/{}".format(self.s3_bucket, self.s3_json_path)
            self.log.info(f"S3 Path: {s3_path}")

            print("--->",s3_path)
            print("-->", s3_json_path)
            
            if self.s3_json_metadatafile:
                formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                s3_json_path
                )
            else:
                formatted_sql = StageToRedshiftOperator.copy_sql_2.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password
                )

            print("--->",formatted_sql)
            redshift.run(formatted_sql)

            self.log.info('StageToRedshiftOperator DONE!')    

            
        else:
            self.log.warning('Fail to established connection with Redshift')
            self.log.info('StageToRedshiftOperator failed')    

