import requests
import pandas as pd
import numpy as np


#Pyspark library
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, split, when, regexp_replace, explode,arrays_zip

# Google Cloud Storage
from google.cloud import storage

# Define Variables
bucket_name = "exercise-data-proj"

# Initialize Spark
def initial_spark():
    # Create Spark Session
    sc = SparkContext()
    spark = SparkSession(sc)
    print(f"The current spark version = { spark.version }.")
    return spark

# Download File
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Create Spark Session
spark = initial_spark()

def exercise_data_download():
    #Download the file from google cloud storage bucket to be used in dataproc
    df = pd.read_excel("gs://.../psdexercise.xlsx")
    df = df.reset_index()

    # Get the information of this dataset
    print("Get information on this dataset.")
    print(df.info())
    pd.DataFrame.iteritems = pd.DataFrame.items
    print(df[df['แรงจูงใจ'].isnull() == True])
    df = df.fillna('ไม่ระบุ')


    #Make the schema for the dataframe
    fields = [StructField(field_name, StringType(), True) for field_name in df.columns]
    schema = StructType(fields)

    #Create Pyspark dataframe
    df = spark.createDataFrame(df, schema)
    return df

 
def cleaned_data(df):
    #Change the column name to prevent confusion
    df = df.withColumnRenamed('เวลา','ช่วงเวลาออกกำลังกาย')\
        .withColumnRenamed('กิจกรรม_2564','กิจกรรม').withColumnRenamed('index','ID')
    df.select(['ช่วงเวลาออกกำลังกาย','กิจกรรม']).show(5)
    
    #Using regexp_replace to remove "(number)" value within the column
    df = df.withColumn('กิจกรรม',regexp_replace('กิจกรรม',"\(\d+\)","")).withColumn('ระดับ',regexp_replace('ระดับ',"\(\d+\)",""))\
        .withColumn('นาที/วัน',regexp_replace('นาที/วัน',"\(\d+\)","")).withColumn('วัน/สัปดาห์',regexp_replace('วัน/สัปดาห์',"\(\d+\)",""))
    df.show(5)

    #Explode the data 
    df = df.withColumn('กิจกรรม',split(df['กิจกรรม'],',')).withColumn('ระดับ',split(df['ระดับ'],','))\
        .withColumn('นาที/วัน',split(df['นาที/วัน'],',')).withColumn('วัน/สัปดาห์',split(df['วัน/สัปดาห์'],','))
    df.show(5)
    
    df = df.withColumn("new",arrays_zip('กิจกรรม','ระดับ','นาที/วัน','วัน/สัปดาห์'))\
    .withColumn("new",explode('new'))
    df.show(5)

    df = df.withColumn('กิจกรรม',df['new.กิจกรรม'])\
    .withColumn('ระดับ',df['new.ระดับ']).withColumn('นาที/วัน',df["new.นาที/วัน"])\
    .withColumn('วัน/สัปดาห์',df['new.วัน/สัปดาห์'])
    df = df.withColumn('เหตุผล (ไม่ออก)',regexp_replace('เหตุผล (ไม่ออก)','-','ไม่ระบุเหตุผล')).drop('new')
    df.select('กิจกรรม','ระดับ','นาที/วัน','วัน/สัปดาห์').show(5)

    #Change the invalid value for google big query
    for col in df.columns:
        df = df.withColumn(col,when(df[col]=='-','ไม่ระบุ เพราะไม่ออกกำลังกาย').otherwise(df[col]))
    df.show(5)

    #Change column data type to match their format
    df = df.withColumn("อายุ",df["อายุ"].cast(IntegerType())).withColumn("น้ำหนัก",df["น้ำหนัก"].cast(IntegerType()))\
    .withColumn("ส่วนสูง",df["ส่วนสูง"].cast(IntegerType())).withColumn("นาที/วัน",df["นาที/วัน"].cast(IntegerType()))\
    .withColumn("วัน/สัปดาห์",df["วัน/สัปดาห์"].cast(IntegerType()))\
    .withColumn("เวลารวม (ชั่วโมง)",df["เวลารวม (ชั่วโมง)"].cast(IntegerType()))

    df.printSchema()

    #Rename the column to English for Google Biq query import
    df = df.withColumnRenamed('ปี',"year").withColumnRenamed('ภาค',"sector").withColumnRenamed('จังหวัด','province')\
        .withColumnRenamed('อำเภอ','district').withColumnRenamed('ตำบล','sub_dtrict').withColumnRenamed('เพศ','gender')\
        .withColumnRenamed('อายุ','age').withColumnRenamed('การศึกษา','education').withColumnRenamed('สถานภาพ','marital_status')\
        .withColumnRenamed('การมีโรคประจำตัว','congenital_disease').withColumnRenamed('ประเภทของโรค','disease_type')\
        .withColumnRenamed('อาชีพ','occupation').withColumnRenamed('น้ำหนัก','weight').withColumnRenamed('ส่วนสูง','height')\
        .withColumnRenamed('กิจกรรม','activity').withColumnRenamed('ระดับ','intensity_level').withColumnRenamed('นาที/วัน','minute_per_day')\
        .withColumnRenamed('วัน/สัปดาห์','day_per_week').withColumnRenamed('รวมสรุปกิจกรรม','activity_summary')\
        .withColumnRenamed('สถานที่','place').withColumnRenamed('ช่วงเวลาออกกำลังกาย','activity_time').withColumnRenamed('เวลารวม (ชั่วโมง)','time_summary')\
        .withColumnRenamed('เหตุผล (ออก)','reason_for_start_activity').withColumnRenamed('เหตุผล (ไม่ออก)','reason_for_not_doing_activity')\
        .withColumnRenamed('แรงจูงใจ','motivation_for_activity').withColumnRenamed('ข้อเสนอแนะ','suggestion')
    df.show(10)

    return(df)

def write_output(cleaneddata):
    print("Write to bucket and big query")
    cleaneddata.write.csv('gs://......../psdexercise.csv',mode="overwrite", header=True)
    cleaneddata.write.format('bigquery') \
        .option("table", "exercise-activity-417105.exercise_activity.exercise-data") \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()
    print("Finished writing to bucket.")


exercise_data = exercise_data_download()
clean_exercise_data = cleaned_data(exercise_data)
write_output(clean_exercise_data)
spark.stop()


    

