from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Description:
      Airflow Operator that will check data tables for presence
      of valid data as a form of quality check

    Parameters:
      tables - list containing all tables to check for data
      redshift_conn_id = airflow connection that contains the Redshift instance credentials

    Returns:
      none    
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Beginning data quality check...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Verifying data quality for {table} table...")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed! {table} table returned no results!")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed! {table} table contained 0 rows!")
            self.log.info(f"Data quality check on {table} table passed with {records[0][0]} records!")