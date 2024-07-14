import download_csv 
import webscraping
import bigquery_handling
import queries
import genAI

def main():
    soup=webscraping.get_soup_from_url()  # Extracting HTML content
    links=webscraping.fetch_link_from_soup(soup, 2019)  # Extracting .parquet links
    download_csv.download_csv_from_link(links)  # downloading csv files from the link
    bigquery_handling.process_yellow_taxi_dataset()  # processing only yellow taxi data
    bigquery_handling.process_green_taxi_dataset()    # processing only green taxi data
    bigquery_handling.process_hired_taxi_dataset()    # processing only hired taxi data
    bigquery_handling.process_high_vol_hired_taxi_dataset()   # processing only high volume hire taxi data


    # Calculate total trips and average fare per day for yellow taxi
    query1 = '''
        SELECT
        DATE(tpep_pickup_datetime) AS trip_date,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS average_fare
        FROM
        `aakash-project-422813.d2k_dataset.yellow_taxi`
        WHERE
        EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
        GROUP BY
        trip_date
        ORDER BY
        trip_date;
        '''
         
    print("Query 1 result..")
    result, message=queries.execute_sql_query(query1)
    print(result.head())


    # What are the peak hours for taxi usage for yellow taxi
    query2='''SELECT
            EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
            COUNT(*) AS total_trips
            FROM
            `aakash-project-422813.d2k_dataset.yellow_taxi`
            GROUP BY
            hour
            ORDER BY
            total_trips DESC;
            ''' 
    print("Query 2 result..")
    result, message=queries.execute_sql_query(query2)
    print(result.head())


    # How does passenger count affect the trip fare for yellow taxi
    query3='''SELECT
            passenger_count,
            AVG(fare_amount) AS average_fare,
            COUNT(*) AS total_trips
            FROM
            `aakash-project-422813.d2k_dataset.yellow_taxi`
            GROUP BY
            passenger_count
            ORDER BY
            passenger_count;
            '''
    print("Query 3 result..")
    result, message=queries.execute_sql_query(query3)
    print(result.head())


    # What are the trends in usage over the year for yellow taxi
    query4='''SELECT
            EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
            COUNT(*) AS total_trips,
            AVG(fare_amount) AS average_fare
            FROM
            `aakash-project-422813.d2k_dataset.yellow_taxi`
            WHERE
            EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019
            GROUP BY
            month
            ORDER BY
            month;'''

    print("Query 4 result..")
    result, message=queries.execute_sql_query(query4)
    print(result)


     # Run query using natural language
    query="Calculate total trips and average fare per day for yellow taxi"
    result, message=genAI.run_query(query)
    print(result.head())
    

if __name__=="__main__":
    main()
        




    

