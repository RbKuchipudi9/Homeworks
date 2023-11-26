import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

from etl_files.credentials_info import gcp_credentials

mountain_time_zone = pytz.timezone('US/Mountain')


def get_credentials():
    bucket_name = "data_center"
    credentials_info = gcp_credentials()
    return credentials_info, bucket_name


def datagcp(credentials_info, gcs_bucket_name):

    gcs_file_path = 'data/{}/outcomes_{}.csv'

    client = storage.Client.from_service_account_info(credentials_info)
    
    bucket = client.get_bucket(gcs_bucket_name)
   
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')

    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df


def data_transform_to_gcp(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCP.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCP.")
    

def preprocessdata(data):
    data['monthyear'] = pd.to_datetime(data['monthyear'])
    data['month'] = data['monthyear'].dt.month
    data['year'] = data['monthyear'].dt.year
    data['sex'] = data['sex_upon_outcome'].replace('Unknown', np.nan)
    data.drop(columns = ['monthyear', 'sex_upon_outcome'], inplace=True)
    mapping = {
    'name': 'animal_name',
    'datetime': 'ts',
    'date_of_birth': 'dob',
    'age_upon_outcome': 'age'
    }
    data.rename(columns=mapping, inplace=True)

    data[['repro_status', 'gender']] = data.sex.str.split(' ', expand=True)
    data.drop(columns = ['sex'], inplace=True)

    data.fillna('unknown', inplace = True)

    return data


def dimensional_modeling():
    credentials_info, bucket_name = get_credentials()

    raw_data = datagcp(credentials_info, bucket_name)
    
    transformed_data = preprocessdata(raw_data)
    
    datatime_dim = transformed_data[['ts','month','year']].drop_duplicates()
    datatime_dim['timekey'] = range(1, len(datatime_dim)+1 )

    dataanimal_dim = transformed_data[['animal_id','animal_name','dob','animal_type','breed','color','repro_status','gender']].drop_duplicates()
    dataanimal_dim['animalkey'] = range(1, len(dataanimal_dim)+1 )

    dataoutcome_dim = transformed_data[['outcome_type']].drop_duplicates()
    dataoutcome_dim['outcomekey'] = range(1, len(dataoutcome_dim)+1 )

    data_combined = transformed_data.merge(dataanimal_dim, how = 'inner', left_on = 'animal_id', right_on = 'animal_id')
    data_combined = data_combined.merge(datatime_dim, how = 'inner', left_on = 'ts', right_on = 'ts')
    data_combined = data_combined.merge(dataoutcome_dim, how = 'inner', left_on = 'outcome_type', right_on = 'outcome_type')

    data_animal_fact = data_combined[['age','animalkey','outcomekey','timekey']]

    dim_animal_path = "transformed_data/dataanimal_dim.csv"
    dim_dates_path = "transformed_data/datatime_dim.csv"
    dim_outcome_types_path = "transformed_data/dataoutcome_dim.csv"
    fct_outcomes_path = "transformed_data/data_animal_fact.csv"

    data_transform_to_gcp(dataanimal_dim, credentials_info, bucket_name, dim_animal_path)
    data_transform_to_gcp(datatime_dim, credentials_info, bucket_name, dim_dates_path)
    data_transform_to_gcp(dataoutcome_dim, credentials_info, bucket_name, dim_outcome_types_path)
    data_transform_to_gcp(data_animal_fact, credentials_info, bucket_name, fct_outcomes_path)