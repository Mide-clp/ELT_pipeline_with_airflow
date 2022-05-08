from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'
    sql = \
        """
            copy {}
            from '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            region '{}'
        """

    def __init__(self,
                 destination_table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 region="",
                 json_path="",
                 ignore_headers=1,
                 delimiter=",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.destination_table = destination_table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.region = region
        self.json_path = json_path
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):
        self.log.info('Implementing staging to redshift')

        aws_hook = AwsBaseHook("aws_credentials", client_type="s3", region_name=self.region)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        file_csv = ""

        if self.file_format == "csv":
            file_csv = "IGNOREHEADER '{}' \n\tDELIMITER '{}'".format(self.ignore_headers, self.delimiter)

        elif self.file_format == "json":
            file_csv = "json '{}'".format(self.json_path)

        else:
            self.log.error("File not supported at the moment")

        self.log.info(f"Copying data to {self.destination_table}")
        formatted_sql = StageToRedshiftOperator.sql.format(
            self.destination_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )

        completed_sql = formatted_sql + file_csv

        redshift_hook.run(completed_sql)

        self.log.info("copy completed")


