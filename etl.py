import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mssql.information_schema import columns


def extract():
    df = pd.read_csv('C:/Users/tobos/Desktop/Simple-ETL-Pipeline/datasets/social-media-impact-on-suicide-rates.csv')
    return df


def transform(data):
    df = pd.DataFrame(data)
    df['Suicide Rate % change since 2010'] = df['Suicide Rate % change since 2010'].round(2)
    df['Twitter user count % change since 2010'] = df['Twitter user count % change since 2010'].round(2)
    df['Facebook user count % change since 2010'] = df['Facebook user count % change since 2010'].round(2)

    df = df.replace(['FMLE'],['Female'])
    df = df.replace(['MLE'],['Male'])

    df = df.rename(columns={'Suicide Rate % change since 2010':'Suicide Rate % (2010-Present)',})
    df = df.rename(columns={'Twitter user count % change since 2010':'Twitter usage % (2010-Present)',})
    df = df.rename(columns={'Facebook user count % change since 2010': 'Facebook usage %( 2010-Present)',})

    return df

def load(data):
    df = pd.DataFrame(data)
    disk_engine = create_engine('sqlite:///social-media-impact-on-suicide-rates-transformed.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace', index=False)




