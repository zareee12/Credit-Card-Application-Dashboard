from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

# Define function to connect to PostgreSQL database
def connect_to_postgres():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='753580',
        host='localhost',
        port='5432'
    )
    return conn

# Define function to read data from PostgreSQL table
def read_from_postgres():
    conn = connect_to_postgres()
    query = "SELECT * FROM credit_record;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Define function for data transformation
def transform_data(df):
    conn = connect_to_postgres()
    query = "SELECT * FROM aplication_record ar JOIN credit_record cr ON ar.id = cr.id;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Define function for data cleansing
def cleanse_data(df):
    # Remove rows with all null values
    df = df.dropna(how='all')
    return df

# Define function to save data back to PostgreSQL
def save_to_postgres(df):
    conn = connect_to_postgres()
    # Insert data into the table
    df.to_sql('dibimbing', conn, if_exists='append', index=False)
    conn.close()

# Define Airflow DAG
dag = DAG(
    "postgres_data_processing",
    description="Process data from PostgreSQL",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 3, 17),
    catchup=False
)

# Define tasks
read_from_postgres_task = PythonOperator(
    task_id="read_from_postgres",
    python_callable=read_from_postgres,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

cleanse_data_task = PythonOperator(
    task_id="cleanse_data",
    python_callable=cleanse_data,
    provide_context=True,
    dag=dag
)

save_to_postgres_task = PythonOperator(
    task_id="save_to_postgres",
    python_callable=save_to_postgres,
    provide_context=True,
    dag=dag
)

# Define task dependencies
read_from_postgres_task >> transform_data_task >> cleanse_data_task >> save_to_postgres_task
