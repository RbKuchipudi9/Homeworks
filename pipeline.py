
# import libraries
import pandas as pd
import argparse
import sys


def readdata(source):
    dataf = pd.read_csv(source)
    return dataf


def datatransform(data):
    datatransform = data.copy()
    datatransform[['Month', 'Year']] = datatransform['MonthYear'].str.split(' ', expand=True)
    datatransform[['Name']] = datatransform[['Name']].fillna('Name_less')
    datatransform[['Outcome Subtype']] = datatransform[['Outcome Subtype']].fillna('Not_Available')
    datatransform.drop(['MonthYear'], axis=1, inplace = True)
    return datatransform


def exportdata(newdata, trget):
    newdata.to_csv(trget)


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('source', help='source csv')
    args_parser.add_argument('trget', help=('trget file'))
    args = args_parser.parse_args()

    print("processing is starting....")
    data = readdata(args.source)
    newdata = datatransform(data)
    exportdata(newdata, args.trget)
    print("processing is finished.....")


