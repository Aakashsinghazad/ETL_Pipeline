import pandas as pd
from google.cloud import bigquery

'''
This module is responsible for ececuting query It has a function that ececute query and send result (dataframe) and message a response

'''

def execute_sql_query(query):
    client = bigquery.Client()

    try:
        result = client.query(query).to_dataframe()
        message = f'The query : {query}\n was successfully executed and returned the above result.\n'

    except Exception as e:
        result = pd.DataFrame({'Output': ['No Output Returned']})
        message = f'The query : {query}\n could not be executed due to exception {e}\n'
    return result, message