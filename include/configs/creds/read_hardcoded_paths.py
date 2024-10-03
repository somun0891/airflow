import json
#/root/airflow/snowpythconn/creds/snow_creds.json

def snow_creds():
    with open("D:\\Airflow_Server\\airflow\\snowpythconn\\creds\\snow_creds.json","r") as f:
         cred = json.load(f)
         return cred

def mssql_creds():
    with open("D:\\Airflow_Server\\airflow\\snowpythconn\\creds\\mssql_creds.json","r") as f:
         cred = json.load(f)
         return cred

