from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):

    sql = \
        """
        INSERT INTO {}
        """

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 mode="",
                 sql="",
                 *args, **kwargs):
        """

        :param redshift_conn_id:
        :param table:
        :param mode: This can either be 'insert' or 'truncate-insert'
        :param sql:
        :param args:
        :param kwargs:
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode
        self.sql = sql

    def execute(self, context):
        self.log.info('Implementing LoadDimensionOperator')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_sql = LoadDimensionOperator.sql.format(self.table) + self.sql

        self.log.info(f"Running {self.mode} data into {self.table}")
        if self.mode == "truncate-insert":
            self.log.info("Deleting rows in table")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        redshift_hook.run(formatted_sql)

        self.log.info(f"Insert into {self.table} completed")