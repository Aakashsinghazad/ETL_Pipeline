# d2k-Technologies-assignment

# Overview:
This project will guide you to fetch 2019 trip data from a "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" website. I have written a bunch of code for web scraping where I have extracted and downloaded the .parquet file. This parquet file is again converted to .csv using "pyarrow" module of Python. Once I have the CSV file, I have created a pandas data frame out of it where I have done cleaning and preprocessing. Once preprocessing is done I have uploaded data to BigQuery for further analysis. 

# Code Structure
![image](https://github.com/user-attachments/assets/d6c37d14-83a6-4424-aac7-a9b2e32a92ae)


# How to run code in your system
1. Create a virtual environment : python -m venv vitrual_env
2. Install modules : pip install -r requirements.txt
3. Create service account to access BigQuery and assing the following roles.
   
   ![image](https://github.com/user-attachments/assets/a8ecdccf-c858-43b2-9804-820f32b32462)
   
4.  Download serviceaccount json key and put it into the working directory. You need to replace the path in bigquery_handling.py to your's json key path.
   ![image](https://github.com/user-attachments/assets/398bd502-e8b6-47c8-ad61-1a14416c0d66)


6.  Run the code in terminal : python app.py

# Recomandation
This code is handling GB's of data. I recomand you to use it on Cloud. Use GCP compute engine with high CPU/GPU (~32 GB) configuration for better performance. I am using pandas to preprocess data it has it's onw limitation to handle number of rows. if you are using it in your local system it might take more then 2-3 hours to completely run the code (from generating data to uploading it to BigQuery). 
I suggest you to run code step-wise.
Divide the workload into two parts. 
a. Run first 3 line of code and comment rest. 

![image](https://github.com/user-attachments/assets/825b4613-268e-4cfc-8b94-277f5b3a9660)

b. Run second half, comment above code now

![image](https://github.com/user-attachments/assets/1f8743eb-de25-4d8f-bed6-561bcb2de094)






