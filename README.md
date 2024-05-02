# Exercise-Activity-Data-Pipeline-Using-GCP

![image](https://github.com/NapojTH/Exercise-Activity-Data-Pipeline-Using-GCP/assets/67892412/4d351b43-eee4-44e0-9818-31b27bee5434)
ETL (Extracted-Transform-Load) data pipeline created by using Google Cloud Platform Service such as Google Cloud Composer, Google Cloud Dataproc, Google Cloud Biq Query and Looker Studio

This project is one of my personal data engineering/machine learning projects regarding the topic of my interest.

## Topic: Thai people's activity and exercise survey data in 2021 and 2022

### Objective: 
1. Create a simple data pipeline using Google Cloud Platform
2. Identify the motivation for each individual for participating in certain activity
3. Finding insight regarding individuals activity/exercise and other information such as their demographics

### Tools:
1. Visual Studio Code
2. Python 3.12 version
3. Apache Spark(Pyspark) version 3.30 for Google Cloud Dataproc
4. Google Cloud Platform Services: Google Cloud Storage, Google Cloud Composer(Airflow 2), Google Cloud Dataproc, Google Cloud Biq Query, and Looker Studio
5. Jupyter Notebook for showing result of Pyspark version 3.5.1

### End-to-end process:
1. Fetch the data from open datasite from Thailand open data site using python request API. 
2. Store the data to Google Cloud Storage as an excel file.
3. Create python/pyspark script for Google DataProc for data wrangling/cleaning.
4. Upload the python script from step 3. to Google Cloud Storage for this process to run Dataproc.
5. Create a dataset and table in Google Cloud Big Query. 
6. Create a DAG airflow using python script (Step 1-5 will be performed by running this script on Google Cloud Composer via airflow) to perform ETL process
   from Extract data via API, Store Data in Google Cloud Storage, Transform data using python/pyspark script via Dataproc, and Load the cleaned data to Google Cloud Big Query.
7. Create a view from dataset and table in Google Cloud Biq Query after the performance of DAG airflow is success.
8. Using the view created in previous step in Looker Studio for creating dashboard and finding insight.

Full process explanation: (Will be post as a blog in Medium, when: TBA)

### Result
![image](https://github.com/NapojTH/Exercise-Activity-Data-Pipeline-Using-GCP/assets/67892412/1b953337-2b8c-4ef2-be3b-71ed7488e53e)

![image](https://github.com/NapojTH/Exercise-Activity-Data-Pipeline-Using-GCP/assets/67892412/f4375fbf-91eb-4819-a6da-fb2194796d45)


### Resources and References:
1. https://data.go.th/dataset/psdexercise
2. Course: Road to Data Engineer 2.0 by DataTh School
3. https://cloud.google.com/dataproc?hl=en
