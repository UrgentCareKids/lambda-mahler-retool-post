import json
import os
import psycopg2
from datetime import datetime, date
from jinja2 import Template
import requests


current_dir = os.path.dirname(os.path.realpath(__file__))

# Construct the full path to the template file
template_path = os.path.join(current_dir, "templates", "bulk_event.json")

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

def handler(event, context):
    body = event['Records'][0]['Sns']['Message']
    body = json.loads(body)
    operation = body['operation']
    print('operation called: ', operation)
    if operation == 'bulk_schedule':
        bulk_schedule()

def proxy_conn():
    user_name = os.environ['username']
    password = os.environ['password']
    rds_proxy_host = os.environ['host']
    db_name = os.environ['engine']
    try:
        conn = psycopg2.connect(host=rds_proxy_host,user=user_name,password=password,dbname=db_name)
        return conn
    except Exception as e:
        print(f'db connection failed: {e}')
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'US/Central';") # Set the desired time zone
    cursor.close()

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

def send_to_mahler(payload, queue_id):
    payload = json.loads(payload)
    url = os.environ['URL']
    headers= {'Content-Type': 'application/x-www-form-urlencoded'}
    payload['username'] = os.environ['USERNAME1']
    payload['key'] = os.environ['MAHLER_KEY']
    try:
        response = requests.post(url, headers=headers, data=payload)
        print(response.text)
    except Exception as e:
        print(f"Error posting to mahler: {str(e)}")
    post_status = json.loads(response.text)
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

