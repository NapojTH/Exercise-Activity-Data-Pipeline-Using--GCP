import requests

# Google Cloud Storage
from google.cloud.storage import Client

# Airflow DAG
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

default_args = {
    'owner': 'owner',
    'start_date': days_ago(1),
    'schedule_interval': None,
    'dag_id': 'dag_id',
    'retries': 1,

    # To email on failure or retry set "email" arg to your
    # email and enable
    "email": "email[at]email.com",
    "email_on_failure": True,
    "email_on_retry": True,
}

PYSPARK_JOB = {
    "reference": {"project_id": "< project_id >"},
    "placement": {"cluster_name": "< defined cluster name >"},
    "pyspark_job": {"main_python_file_uri": "gs://< bucket name >/< python file path >"},
}

with DAG(
    default_args['dag_id'],
    start_date = default_args['start_date'],
    schedule_interval = default_args['schedule_interval'],
    tags = ['exercise']
) as dag:
    dag.doc_md = """
        # Exercise and activity in 2564 and 2565 airflow

        This is the DAG (Directed Acyclic Graph) on downloading the related datasets, and storing in Google Cloud Storage.
    """

def download_dataset():
    exercise_data = requests.get("http://opendata.dpe.go.th/dataset/178218bb-c6ab-491a-95d6-0a112f3450b5/resource/859a77b6-1aab-48c7-89b9-e48e3245cebe/download/psdexercise.xlsx")
    with open('psdexercise.xlsx','wb') as f:
        f.write(exercise_data.content)
        f.close()

def upload_file_to_gcs():
    client = Client()
    bucket = client.bucket('bucket_name')

    #Create a new blob and upload the file's content
    blob = bucket.blob('source_file_name')
    blob.upload_from_filename('target_filename')

    print("The file is uploaded")

#Create Tasks for the Airflow operation
t1 = PythonOperator(
    task_id = 'download_dataset',
    python_callable = download_dataset,
)

t2 = PythonOperator(
    task_id='upload_to_gcs_task',
    python_callable=upload_file_to_gcs,
    dag=dag,
)

t3 = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PYSPARK_JOB, 
    region='us-central1',
    project_id= "project_name"
)

#Setting up dependencies
t1 >> t2 >> t3

