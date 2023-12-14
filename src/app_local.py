import json
import os
import boto3
import psycopg2
from datetime import datetime, date
from jinja2 import Template
import requests
import sys
import time

current_dir = os.path.dirname(os.path.realpath(__file__))

# Construct the full path to the template file
template_path = os.path.join(current_dir, "templates", "bulk_event.json")

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

def handler(operation, queue_id):
    print(sys.argv)
    print('operation called: ', operation)
    if operation == 'bulk_schedule':
        bulk_schedule()
    elif operation == 'string_sender':
        string_sender(int(queue_id))
    else: print('Operation Unknown: ' ,operation, 'queue_id: ', queue_id)


def get_db_params():
    ssm = boto3.client('ssm', region_name='us-east-2')  # Replace 'your-region' with your AWS region
    parameter = ssm.get_parameter(Name='db_easebase_listener', WithDecryption=True)
    db_params = json.loads(parameter['Parameter']['Value'])
    return db_params

def proxy_conn():
    try:
        db_params = get_db_params()
        conn = psycopg2.connect(
            host=db_params['host'],
            user=db_params['user'],
            password=db_params['password'],
            dbname=db_params['database']
        )
        cursor = conn.cursor()
        cursor.execute("SET TIME ZONE 'US/Central';")  # Set the desired time zone
        cursor.close()
        return conn
    except Exception as e:
        print(f'db connection failed: {e}')

def get_masterdata_db_params():
    ssm = boto3.client('ssm', region_name='us-east-2')  # Replace 'your-region' with your AWS region
    parameter = ssm.get_parameter(Name='db_postgres_masterdata_prod', WithDecryption=True)
    db_params = json.loads(parameter['Parameter']['Value'])
    return db_params

def masterdata_conn():
    try:
        db_params = get_masterdata_db_params()
        conn = psycopg2.connect(
            host=db_params['host'],
            user=db_params['user'],
            password=db_params['password'],
            dbname=db_params['database']
        )
        cursor = conn.cursor()
        cursor.execute("SET TIME ZONE 'US/Central';")  # Set the desired time zone
        cursor.close()
        return conn
    except Exception as e:
        print(f'db connection failed: {e}')

# Example usage
connection = proxy_conn()
if connection:
    print("Successfully connected to the database")
else:
    print("Connection failed")
def bulk_schedule():
    with proxy_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
            get_events_query = f"select * from app.gap_mahler_event_queue where operation = 'bulk_schedule' and task_available is true;"
            try:
                cur.execute(get_events_query)
            except Exception as e:
                print(f"Error executing get_events_query: {str(e)}")
            columns = [desc[0] for desc in cur.description]
            bulk_events = cur.fetchall()
            for bulk_event in bulk_events:
                row_dict = dict(zip(columns, bulk_event))
                print(row_dict)
                queue_id = row_dict['queue_id']
                with open(template_path, "r") as template_file:
                    bulk_event_template = Template(template_file.read())
                payload = bulk_event_template.render(json_event=row_dict)
                print(payload)
                send_to_mahler(payload, queue_id)
            cur.close()

def get_api_params():
    ssm = boto3.client('ssm', region_name='us-east-2')  # Replace 'your-region' with your AWS region
    parameter = ssm.get_parameter(Name='mahler_api_conn', WithDecryption=True)
    api_params = json.loads(parameter['Parameter']['Value'])
    return api_params


def send_to_mahler(payload, queue_id):
    payload = json.loads(payload)
    api_params = get_api_params()
    url = api_params['URL']
    headers= {'Content-Type': 'application/x-www-form-urlencoded'}
    payload['username'] = api_params['USERNAME1']
    payload['key'] = api_params['MAHLER_KEY']
    try:
        response = requests.post(url, headers=headers, data=payload)
        print(response.text)
    except Exception as e:
        print(f"Error posting to mahler: {str(e)}")
    post_status = json.loads(response.text)
    print(post_status)
    if post_status['status'] == 'SUCCESS':
        log_event_status(queue_id, payload, post_status)

def log_event_status(queue_id, payload, post_status):
    with proxy_conn() as _targetconnection:
        with _targetconnection.cursor() as cur:
            update_status_call = f"call app.gap_update_event_status({queue_id}, '{json.dumps(payload)}', '{json.dumps(post_status)}');"
            try:
                cur.execute(update_status_call)
            except Exception as e:
                print(f"Error executing update_status_call: {str(e)}")

def string_sender(queue_id):
    # refactor to grab all task available true where sent ts is null wait then send if null otherwise die

    with proxy_conn() as _targetconnection_get:
        with _targetconnection_get.cursor() as cur:
            get_string = f"select pond_id, payload, mahler_id from app.gap_mahler_string_queue where queue_id = {queue_id};"
            try:
                cur.execute(get_string)
            except Exception as e:
                print(f"Error executing getting payload from queue: {str(e)}")
            payload = cur.fetchone()
            print(payload[2])
            pond_id = payload[0]
            tbl_mahler_id = payload[2]
            payload = payload[1]
            mahler_id = payload['client_id']
            print(mahler_id, pond_id, payload)
            if mahler_id == None and tbl_mahler_id == None:
                print('call create patient', pond_id)
                with masterdata_conn() as _targetconnection_masterdata:
                    with _targetconnection_masterdata.cursor() as cur_masterdata:
                        create_mahler_patient = f"call public.mstr_intake_patient_update(0, '{{\"id\":\"{pond_id}\",\"transaction_type\":\"db_patient_update\",\"test_patient\":\"false\"}}');"
                        try:
                            cur_masterdata.execute(create_mahler_patient)
                            _targetconnection_masterdata.commit()
                            time.sleep(5)
                            get_mahler_id = f"select mahler_client_id  from mat_tmp_fast_demographics mtfd left join mahler_id_cx mic on mic.master_id = mtfd.master_id where pond_id = '{pond_id}';"
                            cur_masterdata.execute(get_mahler_id)
                            mahler_client_id = cur_masterdata.fetchone()[0]
                            print(mahler_client_id)
                            try:
                                update_queue = f"update app.gap_mahler_string_queue set mahler_id = {mahler_client_id}, update_ts = current_timestamp where queue_id = {queue_id};"
                                cur.execute(update_queue)
                                _targetconnection_get.commit()
                                if mahler_client_id != None:
                                    string_sender(queue_id)
                            except Exception as e:
                                print(f"Error updating queue with new mahler id: {str(e)}")
                        except Exception as e:
                            print(f"Error executing calling mstr intake update: {str(e)}")
                cur_masterdata.close()
                _targetconnection_masterdata.close()
            elif mahler_id == None and tbl_mahler_id != None:
                print('updating payload with new mahler_id: ', tbl_mahler_id)
                payload['client_id'] = tbl_mahler_id
                send_to_mahler_string(payload, queue_id)
            else:
                send_to_mahler_string(payload, queue_id)

def send_to_mahler_string(payload, queue_id):
    api_params = get_api_params()
    url = api_params['URL']
    headers= {'Content-Type': 'application/x-www-form-urlencoded'}
    payload['username'] = api_params['USERNAME1']
    payload['key'] = api_params['MAHLER_KEY']
    try:
        response = requests.post(url, headers=headers, data=payload)
        print(response.text)
        with proxy_conn() as _targetconnection:
            with _targetconnection.cursor() as cur:
                update_sent_ts = f"update app.gap_mahler_string_queue set sent_ts = current_timestamp, payload = '{json.dumps(payload)}' where queue_id = {queue_id};"
                cur.execute(update_sent_ts)
                cur.close()
    except Exception as e:
        print(f"Error posting to mahler: {str(e)}")
            

handler(sys.argv[1], sys.argv[2])