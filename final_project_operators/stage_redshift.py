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
        IGNOREHEADER {}
        DELIMITER '{}'
    """


    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credential_id="",
                table ="",
                s3_bucket="",
                s3_key="",
                delimeter=",",
                ignore_headers=1,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key = s3_key
        self.delimeter=delimeter
        self.ignore_headers=ignore_headers



    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        metaStoreBackend = MetastoreBackend()
        aws_connection= metaStoreBackend.get_connection(self.aws_credential_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        try:
            self.log.info("Conecting to redshift .. ")
            conn = redshift.get_conn()
            cursor= conn.cursor()
            
            # Executa uma consulta simples para testar a conexão
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            if result:
                self.log.info('Conexão com o Redshift estabelecida com sucesso')
            else:
                self.log.warning('A consulta não retornou resultados, mas a conexão foi estabelecida')

        except Exception as e:
            self.log.error(f'Falha ao conectar ao Redshift: {str(e)}')
            raise

        self.log.info('StageToRedshiftOperator concluído')    

