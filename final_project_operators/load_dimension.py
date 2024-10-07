from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from util.sql_statements import SQL_TRUNCATE_TABLE,SQL_LOAD_USER_DIM_TABLE,SQL_LOAD_SONGS_DIM_TABLE,SQL_LOAD_ARTISTS_DIM_TABLE,SQL_LOAD_TIME_DIM_TABLE

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

        if self.table == "users":
            self.insert_data_by_sql(redshift,
                                    self.table,
                                    SQL_LOAD_USER_DIM_TABLE,
                                    self.insert_mode)

        elif self.table == "songs":
            self.insert_data_by_sql(redshift,
                                self.table,
                                SQL_LOAD_SONGS_DIM_TABLE,
                                self.insert_mode)
        
        elif self.table == "artists":
            self.insert_data_by_sql(redshift,
                                self.table,
                                SQL_LOAD_ARTISTS_DIM_TABLE,
                                self.insert_mode)
            
        elif self.table == "time":
            self.insert_data_by_sql(redshift,
                                self.table,
                                SQL_LOAD_TIME_DIM_TABLE,
                                self.insert_mode)
            

        self.log.info('StageToRedshiftOperator DONE!')    
    
    def insert_data_by_sql(self,sql_helper,table_name,sql_statement,insert_mode):
            
            self.log.info(f"Inserting data on {table_name} ..")    
    
            
            if insert_mode == "truncate-insert":
                sql_truncate_insert = SQL_TRUNCATE_TABLE.format(table=table_name)
                sql_helper.run(sql_truncate_insert)    
                sql_helper.run(sql_statement)
        
            elif insert_mode =="append-only":
                sql_helper.run(sql_statement)


            self.log.info(f"Insert data on {table_name} finished!")    