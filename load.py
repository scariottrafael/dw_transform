import os
import numpy as np
import csv
import pandas as pd
import tables
import psycopg2
import re

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials



def load_from_drive():
    try:
        # Authenticate and create the PyDrive client.
        auth.authenticate_user()
        gauth = GoogleAuth()
        gauth.credentials = GoogleCredentials.get_application_default()
        drive = GoogleDrive(gauth)
        link = 'https://drive.google.com/file/d/1_lHw76vKTlZKbKvf34HqpoLrY83LDHLl/view?usp=sharing'
        fileDownloaded = drive.CreateFile({'id':'1_lHw76vKTlZKbKvf34HqpoLrY83LDHLl'})
        fileDownloaded.GetContentFile('Crimes_-_2022 (1).csv')
        df_crime = pd.read_csv('Crimes_-_2022 (1).csv', header=0, low_memory=False)
    except:
        print("Carga não conseguiu ser feita")

def clean_data():
    # Vamos dropar as colunas X Coordinate, Y Coordinate, Latitude e Longitude
    df_crime = df_crime.drop(['Latitude', 'Longitude', 'X Coordinate', 'Y Coordinate', 'Year'], axis=1)
    # Limpar a coluna Location pois estava vindo com NaN
    df_crime = df_crime.dropna(subset=['Location'], how='any')
    df_crime
    # dim_Time = df[['Date']].drop_duplicates()
    # dim_Time = dim_Time.sort_values(by='Date', ascending=True)

    # id_Time = []
    # id = 1
    # for i in range(len(dim_Time)):
    #     id_Time.append(id)
    #     id += 1

    # dim_Time.insert(0, "id_time", id_Time, True)

    # dim_Time


    

  
