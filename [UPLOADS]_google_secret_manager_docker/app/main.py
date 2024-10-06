from flask import Flask, send_file, jsonify, request
import os
import json
from dotenv import load_dotenv, find_dotenv
import traceback
from google.cloud import secretmanager
import ast
from google.cloud import bigquery
from google.oauth2 import service_account

app = Flask(__name__)
load_dotenv(find_dotenv())

@app.route("/")
def connection_to_secret_manager():
    try:

        gcp_secret_client = secretmanager.SecretManagerServiceClient()
        file_of_creds_url = str(os.getenv('path_to_secret_manager_creds')) + 'versions/latest'
        response_file_of_creds_name = gcp_secret_client.access_secret_version(name = file_of_creds_url)
        file_of_creds = str(response_file_of_creds_name.payload.data.decode('UTF-8'))
        cred_files = ast.literal_eval(file_of_creds)
        cred = service_account.Credentials.from_service_account_info(cred_files)
        client = bigquery.Client(credentials = cred)
        status = 'status: 200 OK'

        return status
    
    except Exception as e:
        return 'status: 400'




