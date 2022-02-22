from ImportSettings import ImportSettings
import configparser
import tzlocal
import pandas as pd
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import time
from io import BytesIO


class AircallAPIImportSettings(ImportSettings):

    def __init__(self, name, import_data_template, execution_date, s3_bucket, target_schema, config_f = './config/db_settings.ini'):

        super().__init__(name, import_data_template, execution_date, s3_bucket)
        self.config_f = config_f
        config = configparser.ConfigParser()
        config.read(config_f)
        self.target_host = config['redshift']['host']
        self.target_port = config['redshift']['port']
        self.target_db = config['redshift']['db']
        self.target_schema = target_schema

        self.target_user, self.target_password = self.get_credentials('RedshiftLoadUserCredentials', 'RedshiftLoadUserName', 'RedshiftLoadUserPW')

        self.set_redshift_connection()

    def set_redshift_connection(self):
        self.target_connection = self.get_redshift_connection(dbname=self.target_db, host=self.target_host, port=self.target_port, user=self.target_user, password=self.target_password)
    
    def retrieve_calls(self, username, password, per_page, execution_date):

        #change values of start_unix and stop_unix to loop through multiple days at once
        start_unix = execution_date
        stop_unix = execution_date

        #create an empty dataframe to assure the same structure
        req = requests.get('https://api.aircall.io/v1/calls', auth=HTTPBasicAuth(username=username, password=password)
            , params={'per_page':per_page, 'from': time.mktime(datetime(2022, 1, 1).timetuple()), 'to' : time.mktime((datetime(2022, 1, 1) + timedelta(days=1)).timetuple()), 'page':1}).json()
        req_json = json.dumps(req['calls'])
        df_raw = pd.json_normalize(json.loads(req_json), max_level=4)
        df_calls = pd.DataFrame(columns=df_raw.columns)

        while start_unix <= stop_unix:
            req = requests.get('https://api.aircall.io/v1/calls', auth=HTTPBasicAuth(username=username, password=password)
            , params={'per_page':per_page, 'from': time.mktime(start_unix.timetuple()), 'to' : time.mktime((start_unix + timedelta(days=1)).timetuple()), 'page':1}).json()
            req_json = json.dumps(req['calls'])
            df_calls = pd.concat([df_calls,pd.json_normalize(json.loads(req_json), max_level=3)], axis=0, ignore_index=True)
            page = 2
            while req['meta']['next_page_link'] != None:
                req = requests.get('https://api.aircall.io/v1/calls', auth=HTTPBasicAuth(username=username, password=password)
                , params={'per_page':per_page, 'from': time.mktime(start_unix.timetuple()), 'to' : time.mktime((start_unix + timedelta(days=1)).timetuple()), 'page':page}).json()
                req_json = json.dumps(req['calls'])
                df_calls = pd.concat([df_calls,pd.json_normalize(json.loads(req_json), max_level=3)], axis=0, ignore_index=True)
                page += 1
            start_unix = start_unix + timedelta(days=1)

        df_calls = df_calls[['id', 'started_at', 'ended_at', 'answered_at', 'duration', 'status', 'direction', 
            'raw_digits', 'archived', 'missed_call_reason', 'number.id', 'number.name', 'number.country', 
            'number.open', 'number.availability_status', 'number.is_ivr', 'user.id', 'user.name', 'user.email', 'user.available',
            'user.availability_status', 'teams']].copy(deep=True)

        df_calls.fillna(0, inplace=True)
        df_calls['started_at'] = df_calls['started_at'].apply(lambda x: datetime.fromtimestamp(float(x), tzlocal.get_localzone()).strftime("%Y-%m-%d %H:%M:%S") if int(x) > 0 else None)
        df_calls['ended_at'] = df_calls['ended_at'].apply(lambda x: datetime.fromtimestamp(float(x), tzlocal.get_localzone()).strftime("%Y-%m-%d %H:%M:%S") if int(x) > 0 else None)
        df_calls['answered_at'] = df_calls['answered_at'].apply(lambda x: datetime.fromtimestamp(float(x), tzlocal.get_localzone()).strftime("%Y-%m-%d %H:%M:%S") if int(x) > 0 else None)
        df_calls['teams_id'] = df_calls['teams'].apply(lambda x: x[0]['id'] if len(x) > 0 else None)
        df_calls['teams_name'] = df_calls['teams'].apply(lambda x: x[0]['name'] if len(x) > 0 else None)
        df_calls.drop(columns = 'teams', axis=1, inplace=True)
        df_calls.reset_index(drop=True, inplace=True)

        temp=BytesIO()
        df_calls.to_csv(temp, sep=',', mode="wb", encoding="UTF-8", index=False)
        temp.seek(0)

        return temp

