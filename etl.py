import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO, format=' %(levelname)s - %(message)s')

def extract(filepath):
    """ Extracts data from a local CSV file """
    try:
        df = pd.read_csv(filepath)
        logging.info('Extracting data from local CSV file, successful!')
        return df
    except Exception as e:
        return logging.error(f'Error reading CSV file: {e}')


def transform(df):
    """ Transforms the data to the desired structure and format:
        - Rounds the percentages to 2 decimal places
        - Replaces gender/acronym values with their full names
        - Renames columns to clearer titles """

    logging.info('Transforming data into desired structure, starting...')

    percentage_columns = [
        'Suicide Rate % change since 2010',
        'Twitter user count % change since 2010',
        'Facebook user count % change since 2010'
    ]
    df[percentage_columns] = df[percentage_columns].round(2)

    acronym_replacements = {'FMLE' : 'Female', 'MLE' : 'Male', 'BTSX' : 'Both sexes'}
    df.replace(acronym_replacements, inplace=True)

    rename_map = {
        'Suicide Rate % change since 2010': 'Suicide Rate % (2010-Present)',
        'Twitter user count % change since 2010': 'Twitter usage % (2010-Present)',
        'Facebook user count % change since 2010': 'Facebook usage %( 2010-Present)'
    }
    df.rename(columns=rename_map, inplace=True)

    logging.info('Transforming data into desired structure, successful!')
    return df

def load(df, db_path, table_name='cal_uni'):
    """ Loads the data into a sqlite database """
    try:
        disk_engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(table_name, disk_engine, if_exists='replace', index=False)
        logging.info('Data successfully loaded!')
    except Exception as e:
        logging.error(f'Error loading data into database: {e}')
        raise

if __name__ == "__main__":

    sqlite_path = 'C:/Users/tobos/Desktop/Projects/simple-etl-pipeline/datasets/social-media-impact-on-suicide-rates-transformed.db'
    csv_path = 'C:/Users/tobos/Desktop/Projects/simple-etl-pipeline/datasets/social-media-impact-on-suicide-rates.csv'

    # Extract data from a csv file
    data = extract(filepath=csv_path)

    # Transform data
    transformed_data = transform(data)

    # Load data into a sqlite database
    load(transformed_data,sqlite_path)



