from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    sql = "SELECT COUNT(*) FROM {}"
    ui_color = '#89CA29'

    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Implementing DataQualityOperator")

        formatted_sql = DataQualityOperator.sql.format(self.table)

        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(formatted_sql)
        records_count = records[0][0]
        if records_count < 1:
            self.log.error(f"Data quality error:  {self.table} table has no records")

        self.log.info(f"{self.table} has {records_count} records")
