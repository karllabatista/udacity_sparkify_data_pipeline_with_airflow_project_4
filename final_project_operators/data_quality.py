from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id =  redshift_conn_id,
        self.tables = kwargs["params"]["tables"]


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_conn_id = PostgresHook("redshift_conn")

        for table in self.tables:
            self.check_greater_than_zero(redshift_conn_id,table)

    def check_greater_than_zero(self,redshift_conn_id,table):
        
        self.log.info(f"Checking {table} ...")
        records = redshift_conn_id.get_records(f"SELECT COUNT(*) FROM {table}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
