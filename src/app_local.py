import json
import os
import boto3
import psycopg2
from datetime import datetime, date
from jinja2 import Template
import requests
import sys


current_dir = os.path.dirname(os.path.realpath(__file__))

# Construct the full path to the template file
template_path = os.path.join(current_dir, "templates", "bulk_event.json")

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

def handler(operation):
    print('operation called: ', operation)
    if operation == 'bulk_schedule':
        bulk_schedule()
    else: print('Operation Unknown: ' ,operation)


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

handler(sys.argv[2])