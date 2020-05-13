from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Description:
      Airflow Operator that will take AWS S3 data and stage into a 
      AWS Redshift instance.

    Parameters:
      redshift_conn_id = airflow connection that contains the Redshift instance credentials
      aws_credentials_id = airflow connection that contains the aws admin credentials
      table = target table to copy data into
      s3_bucket = AWS S3 bucket to retrieve data from
      s3_key = key for bucket data
      json_path = path to json doc that defines path structure
      region = AWS region

    Returns:
      none    
    """
    
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION AS '{}'
        COMPUPDATE OFF
    """
        
    @apply_defaults
    def __init__(self, redshift_conn_id="", aws_credentials_id="", table="",
                 s3_bucket="", s3_key="", json_path="", region="", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.region
        )
        redshift.run(formatted_sql)





