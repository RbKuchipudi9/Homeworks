import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

from etl_files.credentials_info import gcp_credentials

mountain_time_zone = pytz.timezone('US/Mountain')


def data_from_api(limit=50000, order='animal_id'):
    
    base_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    
    api_key = 'f3sr5fowkaem3zkbxkgltjjey'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    
    offset = 0
    all_data = []

    while offset < 157000:  # Assuming there are 156.3k records
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': order,
        }

        response = requests.get(base_url, headers=headers, params=params)
        print("response : ", response)
        current_data = response.json()
    
        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data


def create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    data_list = []
    for entry in data:
        row_data = [entry.get(column, None) for column in columns]
        data_list.append(row_data)

    df = pd.DataFrame(data_list, columns=columns)
    return df


def data_to_gcp(dataframe, bucket_name, file_path):
    
    print("Writing data to GCP...")

    credentials_info = gcp_credentials()

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS with date: {current_date}.")


def main():
    extracted_data = data_from_api(limit=50000, order='animal_id')
    shelter_data = create_dataframe(extracted_data)

    gcs_bucket_name = 'kuchipudi9'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    data_to_gcp(shelter_data, gcs_bucket_name, gcs_file_path)