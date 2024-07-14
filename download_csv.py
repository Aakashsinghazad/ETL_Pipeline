import requests
import os
import pyarrow.parquet as pq
import pyarrow.csv as pc

'''
    The below code will download parquet file into CSV_Data. Before that it will create parquet file one parquet is converted into csv it will delete parquet file
'''

file_name=["Yellow_Taxi_Trip_Records","Green_Taxi_Trip_Records","Hire_Vehicle_Trip_Records","High_Volume_Hire_Vehicle_Trip_Records"]

# Below function will download csv file from the parquet link
def download_csv_from_link(parquet_links):
   DataFolder="CSV_Data"
   for month, urls in parquet_links.items():
        month_folder = os.path.join(DataFolder, month)
        os.makedirs(month_folder, exist_ok=True)

        for i, url in enumerate(urls):
            print(url)
            parquet_file = os.path.join(month_folder, f"{file_name[i]}.parquet")
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(parquet_file, 'wb') as f:
                    f.write(response.content)

                parquet_table = pq.read_table(parquet_file)
                csv_file = os.path.join(month_folder, f"{file_name[i]}.csv")
                pc.write_csv(parquet_table, csv_file)
                os.remove(parquet_file)

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}.")
            except FileNotFoundError as e:
                print(f"File error: {e}.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}.")
