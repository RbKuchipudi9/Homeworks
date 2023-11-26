import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine

from etl_files.credentialsinfo import gcp_credentials

class GCPTransformedData:

    def __init__(self):
        self.bucket_name = 'data_center'

    def getcred(self):
        credentialsinfo = gcp_credentials()
        return credentialsinfo

    def data_transform_from_gcp(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        credentialsinfo = self.get_cred()
        client = storage.Client.from_service_account_info(credentialsinfo)
        bucket = client.get_bucket(self.bucket_name)

        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df



class data_load_to_postgres:

    def __init__(self):
        self.db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',
            'host': '35.238.54.145',
            'port': '5432',
        }

    def getqueries(self, table_name):
        if table_name =="data_animal_dim":
            query = """CREATE TABLE IF NOT EXISTS data_animal_dim (
                            animalkey INT PRIMARY KEY,
                            animal_id VARCHAR,
                            animal_name VARCHAR,
                            dob DATE,
                            animal_type VARCHAR,
                            breed VARCHAR,
                            color VARCHAR,
                            repro_status VARCHAR,
                            gender VARCHAR
                        );
                        """
        elif table_name =="data_outcome_dim":
            query = """CREATE TABLE IF NOT EXISTS data_outcome_dim (
                            outcomekey INT PRIMARY KEY,
                            outcome_type VARCHAR
                        );
                        """
        elif table_name =="data_time_dim":
            query = """CREATE TABLE IF NOT EXISTS data_time_dim (
                            timekey INT PRIMARY KEY, 
                            ts TIMESTAMP, 
                            month INT, 
                            year INT
                        );
                        """
        else:
            query = """CREATE TABLE IF NOT EXISTS data_animal_fact (
                            main_pk SERIAL PRIMARY KEY, 
                            age VARCHAR, 
                            animalkey INT REFERENCES data_animal_dim(animalkey), 
                            outcomekey INT REFERENCES data_outcome_dim(outcomekey), 
                            timekey INT REFERENCES data_time_dim(timekey)
                        );
                        """
        return query

    def connectpostgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def dataload_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        drop_table = f"DROP TABLE {table_name};"
        cursor.execute(drop_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Table insertion completed - {table_name}")
        

def dataload_to_postgres_main(file_name, table_name):
    gcp_loader = GCPTransformedData()
    table_data = gcp_loader.data_transform_from_gcp(file_name)

    postgres_dataloader = data_load_to_postgres()
    table_query = postgres_dataloader.getqueries(table_name)
    postgres_connection = postgres_dataloader.connectpostgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.dataload_into_postgres(postgres_connection, table_data, table_name)