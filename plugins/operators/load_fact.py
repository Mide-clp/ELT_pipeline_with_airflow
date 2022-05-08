from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    sql = \
        """
        INSERT INTO {}
        """
    ui_color = '#F98866'

    def __init__(self,
                 sql="",
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id =redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
