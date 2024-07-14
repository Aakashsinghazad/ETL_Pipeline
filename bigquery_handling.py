import os
import pandas as pd
from google.cloud import bigquery


pd.set_option('future.no_silent_downcasting', True)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "aakash-project-422813-c13d23836375.json"
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
file_name=["Yellow_Taxi_Trip_Records","Green_Taxi_Trip_Records","Hire_Vehicle_Trip_Records","High_Volume_Hire_Vehicle_Trip_Records"]


def process_yellow_taxi_dataset():
    data_folder="CSV_Data"
    file_name = "Yellow_Taxi_Trip_Records.csv"
    table_id = "aakash-project-422813.d2k_dataset.yellow_taxi"
    schema = [
        bigquery.SchemaField('VendorID', 'INTEGER'),
        bigquery.SchemaField('tpep_pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('tpep_dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('passenger_count', 'INTEGER'),
        bigquery.SchemaField('trip_distance', 'FLOAT'),
        bigquery.SchemaField('RatecodeID', 'FLOAT'),
        bigquery.SchemaField('store_and_fwd_flag', 'STRING'),
        bigquery.SchemaField('PULocationID', 'INTEGER'),
        bigquery.SchemaField('DOLocationID', 'INTEGER'),
        bigquery.SchemaField('payment_type', 'INTEGER'),
        bigquery.SchemaField('fare_amount', 'FLOAT'),
        bigquery.SchemaField('extra', 'FLOAT'),
        bigquery.SchemaField('mta_tax', 'FLOAT'),
        bigquery.SchemaField('tip_amount', 'FLOAT'),
        bigquery.SchemaField('tolls_amount', 'FLOAT'),
        bigquery.SchemaField('improvement_surcharge', 'FLOAT'),
        bigquery.SchemaField('total_amount', 'FLOAT'),
        bigquery.SchemaField('congestion_surcharge', 'FLOAT'),
        bigquery.SchemaField('airport_fee', 'FLOAT'),
        bigquery.SchemaField('trip_duration', 'FLOAT'),
        bigquery.SchemaField('average_speed', 'FLOAT')
    ]
    try:
        for month in months:
            month_folder = os.path.join(data_folder, month)
            if os.path.isdir(month_folder):
                print(f"Processing yellow texi files for {month}...")
                csv_file = os.path.join(month_folder, file_name)
                if os.path.exists(csv_file):
                    df = pd.read_csv(csv_file)
                    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
                    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
                    df['airport_fee'] = df['airport_fee'].fillna(0)
                    df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
                    df["trip_duration"] = ((df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds())/60
                    df['trip_duration']=df['trip_duration'].replace(0, pd.NA)
                    df["average_speed"] = df["trip_distance"]/df["trip_duration"]
                    df['average_speed']=df['average_speed'].replace(0, pd.NA)
                    df['average_speed']=df['average_speed'].fillna(df['average_speed'].mean())
                    df['passenger_count']=df['passenger_count'].fillna(int(df['passenger_count'].mean()))
                    load_taxi_data_to_BigQuery(df,schema,table_id)
                    print(f"{month} table is updated to BigQuery..")
                else:
                    print(f"{csv_file} does not exist.")
            else:
                print(f"{month_folder} does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def process_green_taxi_dataset():
    data_folder="CSV_Data"
    file_name = "Green_Taxi_Trip_Records.csv"
    table_id = "aakash-project-422813.d2k_dataset.green_taxi"
    schema = [
        bigquery.SchemaField('VendorID', 'INTEGER'),
        bigquery.SchemaField('lpep_pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('lpep_dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('store_and_fwd_flag', 'STRING'),
        bigquery.SchemaField('RatecodeID', 'FLOAT'),
        bigquery.SchemaField('PULocationID', 'INTEGER'),
        bigquery.SchemaField('DOLocationID', 'INTEGER'),
        bigquery.SchemaField('passenger_count', 'FLOAT'),
        bigquery.SchemaField('trip_distance', 'FLOAT'),
        bigquery.SchemaField('fare_amount', 'FLOAT'),
        bigquery.SchemaField('extra', 'FLOAT'),
        bigquery.SchemaField('mta_tax', 'FLOAT'),
        bigquery.SchemaField('tip_amount', 'FLOAT'),
        bigquery.SchemaField('tolls_amount', 'FLOAT'),
        bigquery.SchemaField('ehail_fee', 'FLOAT'),
        bigquery.SchemaField('improvement_surcharge', 'FLOAT'),
        bigquery.SchemaField('total_amount', 'FLOAT'),
        bigquery.SchemaField('payment_type', 'FLOAT'),
        bigquery.SchemaField('trip_type', 'FLOAT'),
        bigquery.SchemaField('congestion_surcharge', 'FLOAT'),
        bigquery.SchemaField('trip_duration', 'FLOAT'),
        bigquery.SchemaField('average_speed', 'FLOAT')

    ]
    try:
        for month in months:
            month_folder = os.path.join(data_folder, month)
            if os.path.isdir(month_folder):
                print(f"Processing green texi files for {month}...")
                csv_file = os.path.join(month_folder, file_name)
                if os.path.exists(csv_file):
                    df = pd.read_csv(csv_file)
                    df["congestion_surcharge"]=df["congestion_surcharge"].fillna(0)
                    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
                    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
                    df["trip_duration"] = ((df["lpep_dropoff_datetime"] - df["lpep_pickup_datetime"]).dt.total_seconds())/60
                    df['trip_duration']=df['trip_duration'].replace(0, pd.NA)
                    df["average_speed"] = df["trip_distance"]/df["trip_duration"]
                    df['average_speed']=df['average_speed'].replace(0, pd.NA)
                    df['average_speed']=df['average_speed'].fillna(df['average_speed'].mean())
                    df['passenger_count']=df['passenger_count'].fillna(int(df['passenger_count'].mean()))
                    df["ehail_fee"]=df["ehail_fee"].fillna(0)
                    
                    load_taxi_data_to_BigQuery(df,schema,table_id)
                    print(f"{month} table is updated to BigQuery..")
                else:
                    print(f"{csv_file} does not exist.")
            else:
                print(f"{month_folder} does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    




def process_hired_taxi_dataset():
    data_folder="CSV_Data"
    file_name = "Hire_Vehicle_Trip_Records.csv"
    table_id = "aakash-project-422813.d2k_dataset.hired_taxi"
    schema = [
        bigquery.SchemaField('dispatching_base_num', 'STRING'),
        bigquery.SchemaField('pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('dropOff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('PUlocationID', 'FLOAT'),
        bigquery.SchemaField('DOlocationID', 'FLOAT'),
        bigquery.SchemaField('SR_Flag', 'FLOAT'),
        bigquery.SchemaField('Affiliated_base_number', 'STRING'),
        bigquery.SchemaField('trip_duration', 'FLOAT')
    ]
    try:
        for month in months:
            month_folder = os.path.join(data_folder, month)
            if os.path.isdir(month_folder):
                print(f"Processing hired texi files for {month}...")
                csv_file = os.path.join(month_folder, file_name)
                if os.path.exists(csv_file):
                    df = pd.read_csv(csv_file)
                    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
                    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'], errors='coerce')
                    df["trip_duration"] = ((df["dropOff_datetime"] - df["pickup_datetime"]).dt.total_seconds())/60
                    df['PUlocationID'] = df['PUlocationID'].fillna(0)
                    df['DOlocationID'] = df['DOlocationID'].fillna(0)
                    df['SR_Flag'] = df['SR_Flag'].fillna(0)
                    load_taxi_data_to_BigQuery(df,schema,table_id)
                    print(f"{month} table is updated to BigQuery..")
                else:
                    print(f"{csv_file} does not exist.")
            else:
                print(f"{month_folder} does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    

def process_high_vol_hired_taxi_dataset():
    data_folder="CSV_Data"
    file_name = "High_Volume_Hire_Vehicle_Trip_Records.csv"
    table_id = "aakash-project-422813.d2k_dataset.high_vol_hired_taxi"
    schema = [
        bigquery.SchemaField('hvfhs_license_num', 'STRING'),
        bigquery.SchemaField('dispatching_base_num', 'STRING'),
        bigquery.SchemaField('originating_base_num', 'STRING'),
        bigquery.SchemaField('request_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('on_scene_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('PULocationID', 'INTEGER'),
        bigquery.SchemaField('DOLocationID', 'INTEGER'),
        bigquery.SchemaField('trip_miles', 'FLOAT'),
        bigquery.SchemaField('trip_time', 'INTEGER'),
        bigquery.SchemaField('base_passenger_fare', 'FLOAT'),
        bigquery.SchemaField('tolls', 'FLOAT'),
        bigquery.SchemaField('bcf', 'FLOAT'),
        bigquery.SchemaField('sales_tax', 'FLOAT'),
        bigquery.SchemaField('congestion_surcharge', 'FLOAT'),
        bigquery.SchemaField('airport_fee', 'FLOAT'),
        bigquery.SchemaField('tips', 'FLOAT'),
        bigquery.SchemaField('driver_pay', 'FLOAT'),
        bigquery.SchemaField('shared_request_flag', 'STRING'),
        bigquery.SchemaField('shared_match_flag', 'STRING'),
        bigquery.SchemaField('access_a_ride_flag', 'STRING'),
        bigquery.SchemaField('wav_request_flag', 'STRING'),
        bigquery.SchemaField('wav_match_flag', 'STRING'),
        bigquery.SchemaField('trip_duration', 'FLOAT')

    ]
    try:
        for month in months:
            month_folder = os.path.join(data_folder, month)
            if os.path.isdir(month_folder):
                print(f"Processing high vlolume hired texi files for {month}...")
                csv_file = os.path.join(month_folder, file_name)
                if os.path.exists(csv_file):
                    df = pd.read_csv(csv_file)
                    df['request_datetime'] = pd.to_datetime(df['request_datetime'], errors='coerce')
                    df['on_scene_datetime'] = pd.to_datetime(df['on_scene_datetime'], errors='coerce')
                    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
                    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')
                    df["trip_duration"] = ((df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds())/60
                    df['PULocationID'] = df['PULocationID'].fillna(0)
                    df['DOLocationID'] = df['DOLocationID'].fillna(0)
                    df['airport_fee'] = df['airport_fee'].fillna(0)
                    df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
                    
                    load_taxi_data_to_BigQuery(df,schema,table_id)
                    print(f"{month} table is updated to BigQuery..")
                else:
                    print(f"{csv_file} does not exist.")
            else:
                print(f"{month_folder} does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def load_taxi_data_to_BigQuery(df,schema,table_id):

    client = bigquery.Client()
    try:
        table = client.get_table(table_id)
    except Exception as e:
        table_creation = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table_creation)
        print("Table Created Successfully")

    print("writing data into bigquery...")
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() 






