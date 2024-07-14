import os
from google.cloud import bigquery
import openai
import pandas as pd

'''
This module gives user a additional features. User can ask question in Natutal language. It is currently created for yellow taxi records only

'''

os.environ['OPENAI_API_KEY'] = ''

project_id = 'aakash-project-422813'
dataset_id = 'd2k_dataset'
table_id = 'yellow_taxi'

openai.api_key = os.environ['OPENAI_API_KEY']

# Fetch table schema from bigquery
def fetch_table_schema(project_id, dataset_id, table_id):
    bqclient = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bqclient.get_table(table_ref)
    schema_dict = {schema_field.name: schema_field.field_type for schema_field in table.schema}
    return schema_dict

# Generate bigquery query form the given description
def get_sql_query(description):
    schema = fetch_table_schema(project_id, dataset_id, table_id)
    prompt = f'''
    Generate the BigQuery query for the following task:\n{description}.\n
    The database you need is called {dataset_id} and the table is called {table_id}.
    Use the format {dataset_id}.{table_id} as the table name in the queries.
    Enclose column names in backticks(`) not quotation marks.
    Do not assign aliases to the columns.
    Do not calculate new columns, unless specifically called to.
    Return only the BigQuery query, nothing else.
    Do not use WITHIN GROUP clause.
    \nThe list of all the columns is as follows: {schema}
    '''
    try:
        completion = openai.ChatCompletion.create(
            model='gpt-4-turbo',
            messages=[
                {"role": "system", "content": "You are an expert Data Scientist with in-depth knowledge of BigQuery, working on Network Telemetry Data."},
                {"role": "user", "content": prompt},
            ]
        )
        sql_query = completion.choices[0].message['content'].strip().split('```sql')[1].split('```')[0]
    except Exception as e:
        print(f'The following error occurred: {e}\n')
        sql_query = None
    return sql_query

# This function will execute the sql query
def execute_sql_query(query):
    client = bigquery.Client()
    try:
        result = client.query(query).to_dataframe()
        message = f'The query : {query}\n was successfully executed and returned the above result.\n'
    except Exception as e:
        result = pd.DataFrame({'Output': ['No Output Returned']})
        message = f'The query : {query}\n could not be executed due to exception {e}\n'
    return result, message

def run_query(desc):
    query=get_sql_query(desc)
    result, message = execute_sql_query(query)
    return result, message


