import os
import psycopg2
import boto3
import json
import logging
import sys
from datetime import datetime
import uuid
from botocore.exceptions import ClientError

class ImportSettings:
    def __init__(self, name, import_data_template, execution_date, s3_bucket):
        self.name = name
        self.import_data_template = import_data_template
        self.execution_date = execution_date
        self.s3_bucket = s3_bucket
        self.target_connection = None

    def get_redshift_connection(self, dbname, host, port, user, password):
        try:      
            connection = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
        except psycopg2.OperationalError as err:
            logging.error('Error connecting to database: ' + str(err))
            sys.exit(os.EX_SOFTWARE)
        return connection

    def get_credentials(self, secret_id, username_key, password_key):
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name='eu-west-1')
        secret_values = json.loads(client.get_secret_value(SecretId=secret_id)['SecretString'])
        return secret_values[username_key], secret_values[password_key]

    def retrieve_data(self, execution_date):
        raise NotImplementedError

    def push_to_s3(self, file_path, s3_filename, bucket):
        """Writes a pandas df to a given s3 path
        """
        if file_path is None or bucket is None:
            return None
        logging.info('pushing file to s3...')
        s3_client = boto3.client('s3')
        try:
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            uuid_str = str(uuid.uuid4())
            s3_path = 'aircall_calls' + '/' + date[:4] + '/' + s3_filename + '_' + date + '_' + uuid_str + '.csv'
            response = s3_client.upload_fileobj(file_path, bucket, s3_path, ExtraArgs={'ACL': 'bucket-owner-full-control'})
            absolute_s3_path = 's3://' + bucket + '/' + s3_path
            return absolute_s3_path
        except ClientError as e:
            logging.error('error pushing raw data to s3: ' + str(e))
            return None

    def copy_to_redshift(self, redshift_connection, s3_path, schema, table):

        if s3_path is None or schema is None or table is None:
            return False
        columns = ''

        try:
            with open(self.import_data_template) as f:
                columns_template = json.load(f)
        except FileNotFoundError as err:
            logging.error('Error finding import data template: ' + str(err))
            return False
        for i, key in enumerate(columns_template['columns']):
            if i < len(columns_template['columns'])-1:
                columns += key['name'] + ' ' + key['type'] + ','
            else:
                columns += key['name'] + ' ' + key['type']

        create_statement = 'CREATE TABLE IF NOT EXISTS ' + schema + '.' + 'import_' + table + ' ( ' + columns + ');' 
        copy_statement = "COPY " + schema + '.' + 'import_' + table + " FROM '" + s3_path + "' iam_role 'arn:aws:iam::449517182508:role/RedshiftCopy' CSV IGNOREHEADER 1 DELIMITER ',' ACCEPTINVCHARS TRUNCATECOLUMNS;"
        logging.info('create statement: ' + str(create_statement))
        logging.info('copy statement: ' + str(copy_statement))

        cur = redshift_connection.cursor()

        try:
            cur.execute(create_statement)
        except Exception as err:
            logging.error("error creating table: " + str(err))
            cur.close()
            return False

        try:
            cur.execute(copy_statement)
        except Exception as err:
            logging.error("error copying to Redshift: " + str(err))
            cur.close()
            return False

        redshift_connection.commit()
        cur.close()

        return True

    def run(self, execution_date):

        if execution_date is None:
            execution_date = (datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        # ensure the execution date we use in the query is valid
        try:
            execution_date_iso = datetime.fromisoformat(execution_date)
            execution_date_str = datetime.strftime(execution_date_iso, '%Y-%m-%d')
            execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d')

        except ValueError as err:
            logging.error('Error parsing execution date: ' + str(err))
            sys.exit(os.EX_CONFIG)

        aircall_api_token, aircall_api_password = self.get_credentials('aircall_user', 'api_token', 'api_password')

        local_filepath = self.retrieve_calls(aircall_api_token, aircall_api_password, 50, execution_date)
        s3_path = self.push_to_s3(local_filepath, self.name, self.s3_bucket)

        if s3_path is None:
            logging.error('Error pushing data to s3')
            sys.exit(os.EX_SOFTWARE)
        logging.info('pushed to s3 path: ' + str(s3_path))
        redshift_copy_result = self.copy_to_redshift(self.target_connection, s3_path, self.target_schema, self.name)

        if redshift_copy_result is False:
            logging.error('Error copying to Redshift')
            sys.exit(os.EX_SOFTWARE)

        local_filepath.close()
