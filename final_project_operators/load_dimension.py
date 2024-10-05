from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from util.sql_statements import SQL_TRUNCATE_TABLE,SQL_LOAD_USER_DIM_TABLE

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_mode="truncate-insert",
                 table ="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.insert_mode = insert_mode
        self.table = table

    def execute(self, context):
        self.log.info("Starting LoadDimensionOperator ..")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.insert_mode == "truncate-insert":
            sql_truncate_insert = SQL_TRUNCATE_TABLE.format(table=self.table)
            redshift.run(sql_truncate_insert)    
            redshift.run(SQL_LOAD_USER_DIM_TABLE)
        
        elif self.insert_mode =="append-only":
            redshift.run(SQL_LOAD_USER_DIM_TABLE)

        self.log.info('StageToRedshiftOperator DONE!')    
