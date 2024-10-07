import telebot
from telebot import types
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import json

load_dotenv(find_dotenv())

class Tgmessage:
    def __init__(self, send, chatid):
        self.send = send
        self.chatid = chatid

    def message_alarm(self):
        bot = telebot.TeleBot(str(os.getenv('bot_id')))
        bot.send_message(self.chatid, self.send)


class SQLQuery:
    def __init__(self, dataset_id, table_id, destination):
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.destination = destination
        self.page_id_list = []

    def insert_to_bq(self, df_for_insert):
        self.file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(self.file_of_creds.read())
        self.file_of_creds.close()
        self.cred = service_account.Credentials.from_service_account_info(json_account_info)
        self.client = bigquery.Client(credentials=self.cred)

        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table(self.table_id)
        job_config = bigquery.LoadJobConfig()
        if self.destination == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif self.destination == 'WRITE_APPEND':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = self.client.load_table_from_dataframe(df_for_insert, table, job_config=job_config, parquet_compression='snappy')

    def exists_table(self):
        self.file_of_creds = open(str(os.getenv('path')), "r")
        json_account_info = json.loads(self.file_of_creds.read())
        self.file_of_creds.close()
        self.cred = service_account.Credentials.from_service_account_info(json_account_info)
        self.client = bigquery.Client(credentials=self.cred)
        self.table_max_dt = str(os.getenv('project_id')) + str('.') + str(self.dataset_id) + str('.') + str(self.table_id)
        query_string = """SELECT distinct date_add(date(max(datetime(date))), interval +1 day) as date FROM `{}`""".format(self.table_max_dt)
        max_dt = self.client.query(query_string).result().to_dataframe(create_bqstorage_client=True)
        return max_dt