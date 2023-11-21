import json
import boto3
import psycopg2
import select
import subprocess

# Configure AWS
boto3.setup_default_session(region_name='us-east-2')
ssm = boto3.client('ssm')

async def get_db_params():
    params = {
        'Name': 'db_postgres_easebase_internal',
        'WithDecryption': True
    }

    response = ssm.get_parameter(**params)
    db_params = json.loads(response['Parameter']['Value'])
    db_params['sslmode'] = 'require'
    return db_params

async def run_app_py():
    subprocess.run(["python3", "src/app_local.py"])

async def main():
    db_params = await get_db_params()
    connection = psycopg2.connect(**db_params)
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("LISTEN mahler_retool")

    print("Listening...")

    try:
        while True:
            if select.select([connection], [], [], 5) == ([], [], []):
                print("Timeout")
            else:
                connection.poll()
                while connection.notifies:
                    notify = connection.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)

                    # Run app.py upon receiving a notification
                    await run_app_py()

    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
