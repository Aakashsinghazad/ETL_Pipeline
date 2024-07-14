import download_csv 
import webscraping
import bigquery_handling

def main():
    # soup=webscraping.get_soup_from_url()  # Extracting HTML content
    # links=webscraping.fetch_link_from_soup(soup, 2019)  # Extracting .parquet links
    # download_csv.download_csv_from_link(links)  # downloading csv files from the link
    bigquery_handling.process_yellow_taxi_dataset()  # processing only yellow taxi data
    bigquery_handling.process_green_taxi_dataset()    # processing only green taxi data
    bigquery_handling.process_hired_taxi_dataset()    # processing only hired taxi data
    bigquery_handling.process_high_vol_hired_taxi_dataset()   # processing only high volume hire taxi data


if __name__=="__main__":
    main()
        




    

