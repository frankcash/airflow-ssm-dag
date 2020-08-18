from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
import boto3
import json


@provide_session
def create_connections(session=None):
    """
    create_connections creates Airflow Connections utilizing the Airflow model
    for connections.  It will use get_secret(...) to call the desired secret
    manager.  When grabbing credentials create_connection will look for very
    specific fields, not all of which are used.
    host
    port
    username
    password
    engine: source type i.e. Postgres or S3
    dbClusterIdentifier: will become the conn_id
    """
    secrets = [ ] # TODO: fill in secrets
    for secret in secrets:
        source = get_secret(secret)

        host = source.get("host", "")
        port = source.get("port", "5432")
        db = source.get("dbname", "")
        user = source.get("username", "")
        password = source.get("password", "")
        engine = source.get("engine", "postgres")
        name = source.get("dbClusterIdentifier", secret)
        try:
            connection_query = session.query(Connection).filter(
                Connection.conn_id == name, )
            connection_query_result = connection_query.one_or_none()
            if not connection_query_result:
                connection = Connection(conn_id=name,
                                        conn_type=engine,
                                        host=host,
                                        port=port,
                                        login=user,
                                        password=password,
                                        schema=db)
                session.add(connection)
                session.commit()
            else:
                connection_query_result.host = host
                connection_query_result.login = user
                connection_query_result.schema = db
                connection_query_result.port = port
                connection_query_result.set_password(password)
                session.add(connection_query_result)
                session.commit()
        except Exception as e:
            raise e


def get_secret(secret_name):
    """
    get_secret utilizes boto3 session to connect to ssm and then fetch desired
    secret

    :param secret_name: desired secret to fetch from ssm
    :return: secret in json or None
    """
    region_name = "us-east-1"
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager',
                                region_name=region_name)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            return None
    except Exception as e:
        raise e


default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('conn_sync_dag_0_0_1',
          description='Syncs Connections from SSM',
          schedule_interval='5 */12 * * *',
          start_date=datetime(2018, 1, 20),
          catchup=False)

connection_sync = PythonOperator(task_id='create_connections_0_0_1',
                                 python_callable=create_connections,
                                 dag=dag)

connection_sync
