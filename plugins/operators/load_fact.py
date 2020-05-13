from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Description:
      Airflow Operator that will read staged data in Redshift and  
      insert into a structured facts table

    Parameters:
      table - Name of the target facts table
      sql = sql query for insert to target table
      redshift_conn_id = airflow connection that contains the Redshift instance credentials

    Returns:
      none    
    """
    
    ui_color = '#F98866'
    
    sql_insert = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f'Loading data into {self.table} fact table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.sql_insert.format(self.table, self.sql)
        redshift.run(formatted_sql)