from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from util.sql_statements import SQL_LOAD_FACT_TABLE

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                 *args, **kwargs):
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id

    def execute(self, context):
        self.log.info('Starting Load Fact Table ..')
        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(SQL_LOAD_FACT_TABLE)

        self.log.info('LoadFactOperator DONE!')    
