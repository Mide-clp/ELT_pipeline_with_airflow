
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')





