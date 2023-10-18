
# import libraries
import pandas as pd
import numpy as np
import argparse
import sys
from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import sessionmaker


def readdata(source):
    dataf = pd.read_csv(source)
    return dataf



def datatransform(data):
    datanew = data.copy()
    datanew[['month', 'year']] = datanew.MonthYear.str.split(' ', expand=True)
    datanew['sex'] = datanew['Sex upon Outcome'].replace('Unknown', np.nan)
    datanew.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    mapping = {
    'Animal ID': 'animal_id',
    'Name': 'animal_name',
    'DateTime': 'date_recorded',
    'Date of Birth': 'dofb',
    'Outcome Type': 'outcome_type',
    'Outcome Subtype': 'outcome_subtype',
    'Animal Type': 'animal_type',
    'Age upon Outcome': 'age',
    'Breed': 'breed',
    'Color': 'color'
    }
    datanew.rename(columns=mapping, inplace=True)
    datanew[['repro_status', 'gender']] = datanew.sex.str.split(' ', expand=True)
    datanew.drop(columns = ['sex'], inplace=True)

    datanew.fillna('unknown', inplace = True)

    return datanew



def exportdata(data):

  

    db_url = "postgresql+psycopg2://roja:datacenter@db:5432/shelter"
    connection = create_engine(db_url)

   

    time_dim = data[['date_recorded','month','year']]
    time_dim['timekey'] = range(1, len(time_dim)+1 )
    time_dim.to_sql("timedim", connection, if_exists="append", index = False)

    animal_dim = data[['animal_id','animal_name','dofb','animal_type','breed','color','repro_status','gender']]
    animal_dim['animalkey'] = range(1, len(animal_dim)+1 )
    animal_dim.to_sql("animaldim", connection, if_exists="append", index = False)

    outcome_dim = data[['outcome_type','outcome_subtype']].drop_duplicates()
    outcome_dim['outcomekey'] = range(1, len(outcome_dim)+1 )
    outcome_dim.to_sql("outcomedim", connection, if_exists="append", index = False)
    
    data_comb = data.merge(animal_dim, how = 'inner', left_on = 'animal_id', right_on = 'animal_id')
    data_comb = data_comb.merge(time_dim, how = 'inner', left_on = 'date_recorded', right_on = 'date_recorded')
    data_comb = data_comb.merge(outcome_dim, how = 'inner', left_on = ['outcome_type','outcome_subtype'], right_on = ['outcome_type','outcome_subtype'])

    animal_fact = data_comb[['age','animalkey','outcomekey','timekey']]
    animal_fact.to_sql("animalfact", connection, if_exists="append", index = False)  


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('source', help='source csv')
# args_parser.add_argument('trget', help=('trget file'))
    args = args_parser.parse_args()

    print("processing is starting....")
    df = readdata(args.source)
    dfnew = datatransform(df)
    exportdata(dfnew)
    print("processing is finished.....")


