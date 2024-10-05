import os
import json
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

load_dotenv(find_dotenv())

class GDriveBqUploader:
    def __init__(self, table_name, url_for_download, table_id):
        self.table_name = table_name
        self.url_for_download = url_for_download
        self.table_id = table_id

    def upload_to_bq(self):
        file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(file_of_creds.read())
        file_of_creds.close()
        cred = service_account.Credentials.from_service_account_info(json_account_info)
        client = bigquery.Client(credentials=cred)

        #if need drop-create sql query
        query_string = """drop table if exists `{}`;""".format(str(self.table_name))
        result_of_drop = client.query(query_string).result()

        df_to_bq = pd.read_csv(str(self.url_for_download), error_bad_lines=False, delimiter=',')

        dataset = client.dataset(str(os.getenv('dataset_id')))
        table = dataset.table(str(self.table_id))
        job_config = bigquery.LoadJobConfig()
        destination = 'WRITE_TRUNCATE'
        job = client.load_table_from_dataframe(df_to_bq, table, job_config=job_config, parquet_compression='snappy')

        #if need name-value table:
        df_name_value = pd.melt(df, var_name = 'column_name')
        df_name_value = df_name_value.dropna()
        df_name_value = df_name_value.rename(columns = {'value': 'column_value'})
        df_name_value = df_name_value.reset_index(drop = True)

        return 'status: 200 OK'




