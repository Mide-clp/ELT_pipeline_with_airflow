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

        """

        :param sql:
        :param redshift_conn_id:
        :param table:
        :param args:
        :param kwargs:
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('Implementing load fact table')

        postgres_hook = PostgresHook("redshift")

        formatted_sql = LoadFactOperator.sql.format(self.table) + self.sql

        postgres_hook.run(formatted_sql)

        self.log.info(f"Insert completed for {self.table} table")
