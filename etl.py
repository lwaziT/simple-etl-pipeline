import pandas as pd
from sqlalchemy import create_engine
<<<<<<< HEAD
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

=======


def extract():
    """ Extracts data from local CSV file """
    df = pd.read_csv('C:/Users/tobos/Desktop/Simple-ETL-Pipeline/datasets/social-media-impact-on-suicide-rates.csv')
    print('Extracting data from local CSV file, successful!')
    return df

def transform(dataset):
    """ Transforms the data to desired structure and format:
        Renames some of the columns, writes out acronyms,
        and rounds off percentages to two decimal places. """
    df = pd.DataFrame(dataset)
    df['Suicide Rate % change since 2010'] = df['Suicide Rate % change since 2010'].round(2)
    df['Twitter user count % change since 2010'] = df['Twitter user count % change since 2010'].round(2)
    df['Facebook user count % change since 2010'] = df['Facebook user count % change since 2010'].round(2)

    df = df.replace(['FMLE'], ['Female'])
    df = df.replace(['MLE'], ['Male'])
    df = df.replace(['BTSX'], ['Both sexes'])

    df = df.rename(columns={'Suicide Rate % change since 2010': 'Suicide Rate % (2010-Present)', })
    df = df.rename(columns={'Twitter user count % change since 2010': 'Twitter usage % (2010-Present)', })
    df = df.rename(columns={'Facebook user count % change since 2010': 'Facebook usage %( 2010-Present)', })
    print('Transforming data into desired structure, successful!')
    return df

def load(dataset):
    """ Loads the data into a sqlite database """
    df = pd.DataFrame(dataset)
    disk_engine = create_engine('sqlite:///C:/Users/tobos/Desktop/Simple-ETL-Pipeline/datasets/social-media-impact-on-suicide-rates-transformed.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace', index=False)
    print('Data successfully loaded!')

data = extract()
dataframe = transform(data)
load(dataframe)
>>>>>>> d9411a4e990edc1ad459b0b212d67995d5b6a68a


