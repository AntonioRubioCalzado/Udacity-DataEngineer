from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    query_count = """SELECT COUNT(*) FROM {}"""
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[],
                 provide_context=True,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(DataQualityOperator.query_count.format(table))
            if records is None:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            if num_records < 1:
                self.log.error(f"{table} has 0 records")
                raise ValueError(f"Data quality check failed: {table} has 0 records")
            self.log.info(f"Data quality passed: {table} has {num_records} records")