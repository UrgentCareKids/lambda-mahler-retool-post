import json
import boto3
import psycopg2
import select
import subprocess
import time

# Configure AWS
boto3.setup_default_session(region_name='us-east-2')
ssm = boto3.client('ssm')

def get_db_params():
    params = {
        'Name': 'db_postgres_easebase_internal',
        'WithDecryption': True
    }

    response = ssm.get_parameter(**params)
    db_params = json.loads(response['Parameter']['Value'])
    db_params['sslmode'] = 'require'
    return db_params

def run_app_py(operation):
    subprocess.run(["python3", "/home/ubuntu/repos/lambda-mahler-retool-post/src/app_local.py", operation, queue_id])

def main():
    try:
        db_params = get_db_params()
        connection = psycopg2.connect(**db_params)
        connection.autocommit = True

        cursor = connection.cursor()
        cursor.execute("LISTEN mahler_retool")

        print("Listening...")

        while True:
            if select.select([connection], [], [], 5) == ([], [], []):
                print("Timeout")
            else:
                connection.poll()
                while connection.notifies:
                    notify = connection.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                    payload_data = json.loads(notify.payload)
                    print(payload_data)
                    operation_value = payload_data['operation']
                    queue_id = payload_data['queue_id']

                    # Run app.py with the operation value
                    run_app_py(operation_value, queue_id)

            # Sleep to prevent high CPU usage and allow for interrupt
            time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()

main()

