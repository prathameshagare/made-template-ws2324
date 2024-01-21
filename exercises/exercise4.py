import os
from pathlib import Path
import shutil
import time
from typing import Callable, Any
import urllib.request
import zipfile
import pandas as pd
from sqlalchemy import BIGINT, FLOAT, TEXT


def retrieve_and_unzip_data(url: str, max_tries: int = 5, sec_wait_before_retry: float = 5) -> str:
    # Filename and -path definitions
    data_name = Path(url).stem
    extract_path = os.path.join(os.curdir, data_name)
    zip_name = data_name + '.zip'
    # Download zip with retries
    for i in range(1, max_tries+1):
        try:
            urllib.request.urlretrieve(url, zip_name)
            break
        except:
            print(f'Couldn\'t load zip from given url! (Try {i}/{max_tries})')
            if i < max_tries: time.sleep(sec_wait_before_retry)
    # Check if download was successfull
    if not os.path.exists(zip_name):
        raise FileNotFoundError(f'Failed to load zip from url {url}')
    # Extract and delete zip file
    with zipfile.ZipFile(zip_name, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    os.remove(zip_name)
    # Return path to extracted data
    return extract_path


def validate_data(dataframe: pd.DataFrame, column: str, constraint: Callable[[Any], bool]) -> pd.DataFrame:
    dataframe = dataframe.loc[dataframe[column].apply(constraint)]
    return dataframe


def convert_celsius_to_fahrenheit(temp_cels: float) -> float:
    return (temp_cels * 9/5) + 32


if __name__ == '__main__':
    data_zip_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
    csv_filename = 'data.csv'
    
    # Download and unzip data
    extracted_data_path = retrieve_and_unzip_data(data_zip_url)
    
    # Read and reshape data
    dataframe = pd.read_csv(os.path.join(extracted_data_path, csv_filename),
                     sep=';',
                     index_col=False,
                     usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv'],
                     decimal=',')
    
    # Rename columns
    dataframe.rename(columns={
        'Temperatur in °C (DWD)': 'Temperatur',
        'Batterietemperatur in °C':'Batterietemperatur'
        }, inplace=True)
    
    # Transform data
    dataframe['Temperatur'] = convert_celsius_to_fahrenheit(dataframe['Temperatur'])
    dataframe['Batterietemperatur'] = convert_celsius_to_fahrenheit(dataframe['Batterietemperatur'])
    
    # Validate data 
    dataframe = validate_data(dataframe, 'Geraet', lambda x: x > 0)
    dataframe = validate_data(dataframe, 'Monat', lambda x: x in range(1, 13))
    dataframe = validate_data(dataframe, 'Temperatur', lambda x: -459.67 < x < 212)
    dataframe = validate_data(dataframe, 'Batterietemperatur', lambda x: -459.67 < x < 212)
    dataframe = validate_data(dataframe, 'Geraet aktiv', lambda x: x in ['Ja', 'Nein'])
    
    # Write data into SQLite sql_database
    sql_table = 'temperatures'
    sql_database = 'temperatures.sqlite'
    dataframe.to_sql(sql_table, f'sqlite:///{sql_database}', if_exists='replace', index=False, dtype={
        'Geraet': BIGINT,
        'Hersteller': TEXT,
        'Model': TEXT,
        'Monat': BIGINT,
        'Temperatur': FLOAT,
        'Batterietemperatur': FLOAT,
        'Geraet aktiv': TEXT
    })
    
    # Delete downloaded data
    shutil.rmtree(extracted_data_path)
    
    print('Datapipeline completed successfully..')
    print(f'Data is stored in sql_table "{sql_table}" in sql_database "{sql_database}"')