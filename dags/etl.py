import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Function to read CSV
def read_csv():
    current_dir = os.path.dirname(__file__)
    csv_file = os.path.join(current_dir, "application_record.csv")
    df = pd.read_csv(csv_file)
    return df

# Function to perform format standardization
def format_standardization(df, **kwargs):
    df_transform1 = df.copy()
    df_transform1['YEARS_BIRTH'] = abs(df_transform1['DAYS_BIRTH'] / 365.25).astype(int)
    df_transform1['YEARS_EMPLOYED'] = abs(df_transform1['DAYS_EMPLOYED'] / 365.25).astype(int)
    df_transform1.drop(columns=['DAYS_BIRTH', 'DAYS_EMPLOYED'], inplace=True)
    df_transform1['CODE_GENDER'] = df_transform1['CODE_GENDER'].apply(lambda x: 1 if x == 'F' else 0)
    df_transform1['FLAG_OWN_CAR'] = df_transform1['FLAG_OWN_CAR'].apply(lambda x: 1 if x == 'Y' else 0)
    df_transform1['FLAG_OWN_REALTY'] = df_transform1['FLAG_OWN_REALTY'].apply(lambda x: 1 if x == 'Y' else 0)
    return df_transform1

# Function to cleanse null data
def cleanse_null_data(df, **kwargs):
    df_transform3 = df.dropna(how='all')
    return df_transform3

# Function to save cleansed data to a new CSV file
def save_data(df, **kwargs):
    save_path = "application_record_cleansed.csv"
    df.to_csv(save_path, index=False)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    'application_record_processing',
    default_args=default_args,
    description='Process application record data',
    schedule_interval=timedelta(minutes=5),
)

# Define task to read CSV
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

# Define task to perform format standardization
format_standardization_task = PythonOperator(
    task_id='format_standardization',
    python_callable=format_standardization,
    provide_context=True,
    dag=dag,
)

# Define task to cleanse null data
cleanse_null_data_task = PythonOperator(
    task_id='cleanse_null_data',
    python_callable=cleanse_null_data,
    provide_context=True,
    dag=dag,
)

# Define task to save cleansed data
save_data_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag,
)

# Define dependencies between tasks
read_csv_task >> format_standardization_task >> cleanse_null_data_task >> save_data_task
