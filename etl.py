import pandas as pd
from sqlalchemy import create_engine


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


