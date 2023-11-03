#----------------------------------------------Libs------------------------------------------------------
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib 
# instal- upload data to drive 
import numpy as np
import csv
import pandas as pd
import tables
import psycopg2
import re
import holidays
import datetime

from geopy.distance import great_circle

from google.oauth2 import service_account
from googleapiclient.discovery import build
#---------------------------------------------------Functions-------------------------------------------------
def handle_scd_type2(source_df, target_df, attributes):
    # Create a copy of the source DataFrame to avoid modifying the original data
    final_df = source_df.copy()

    # Create a list to store new rows
    new_rows = []

    # Iterate over the rows of the target DataFrame
    for index, row in target_df.iterrows():
        # If the index is in the source DataFrame, check if
        #  any attribute has changed
        if index in final_df.index:
            for attribute in attributes:
                if final_df.loc[index, attribute] != row[attribute]:
                    # If an attribute has changed, update the 'end_date' and 'is_active' of the old record
                    final_df.loc[index, 'end_date'] = datetime.datetime.now()
                    final_df.loc[index, 'is_active'] = False
                    # Add a new record with the updated attribute
                    new_row = row.copy()
                    new_row['start_date'] = datetime.datetime.now()
                    new_row['end_date'] = datetime.datetime(9999, 12, 31)
                    new_row['is_active'] = True
                    new_rows.append(new_row)
                    break
        else:
            # If the index is not in the source DataFrame, add the row
            new_row = row.copy()
            new_row['start_date'] = datetime.datetime.now()
            new_row['end_date'] = datetime.datetime(9999, 12, 31)
            new_row['is_active'] = True
            new_rows.append(new_row)

    # Concatenate the new rows with the final DataFrame
    final_df = pd.concat([final_df, pd.DataFrame(new_rows)], ignore_index=True)

    # Return the final DataFrame
    return final_df

def is_weekend(day_str):
        status = bool
        dia, mes, ano = int, int, int
        datetimme_splitv = day_str.split(' ')
        partes_data = datetimme_splitv[0].split('/')

        if len(partes_data) != 3:
            print("Formato de data inválido. Use o formato dia/mês/ano.")
            return None

        try:
            dia = int(partes_data[1])
            mes = int(partes_data[0])
            ano = int(partes_data[2])
        except ValueError:
            print("Formato de data inválido. Certifique-se de que são números inteiros.")

        date_day = datetime.date(ano, mes, dia)

        if date_day.weekday() != 5 or date_day.weekday() != 6:
            status = False
        elif date_day.weekday() == 5 or date_day.weekday() == 6:
            status = True
        return status

def is_holiday(day_str):
        us_holidays = holidays.US()
        dia, mes, ano = int, int, int
        datetimme_splitv = day_str.split(' ')
        partes_data = datetimme_splitv[0].split('/')

        if len(partes_data) != 3:
            print("Formato de data inválido. Use o formato dia/mês/ano.")
            return None

        try:
            dia = int(partes_data[1])
            mes = int(partes_data[0])
            ano = int(partes_data[2])
        except ValueError:
            print("Formato de data inválido. Certifique-se de que são números inteiros.")

        date_day = datetime.date(ano, mes, dia)

        satats = date_day in us_holidays
        return satats

def time_to_period(date_str):
        horas, min = int, int
        period = str
        datetime_split = date_str.split(' ')

        try:
            horas = int(datetime_split[1][:2])
            min = int(datetime_split[1][3:5])
        except ValueError:
            print("Formato de tempo inválido. Certifique-se de que são números inteiros.")

        if datetime_split[2] == 'AM':
            if 6 <= horas < 12:
                period = "morning"
            elif horas == 12 and min == 0:
                period = "morning"
            elif horas == 12 and min > 0:
                period = "afternoon"
            else:
                period = "dawn"
        elif datetime_split[2] == 'PM':
            if 6 <= horas < 12:
                period = "night"
            elif horas == 12 and min > 0:
                period = "dawn"
            else:
                period = "afternoon"

        return period

def ward_to_zone(ward):
        zone = str
        north = [26, 29, 30, 31, 32, 33, 35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50]
        south = [5, 6, 7, 8, 9, 10, 17, 18, 19, 20, 21, 34]
        center = [1, 2, 3, 4, 11, 12, 13, 14, 15, 16, 22, 23, 24, 25, 27, 28, 42]

        if ward in north:
            zone = 'north'
        elif ward in center:
            zone = 'center'
        elif ward in south:
            zone = 'south'
        else:
            print('Zona não registrada no catalogo de zonas', ward )

        return zone

def split_date_time(date_time):
    date = date_time.split(' ')[0]
    time = date_time.split(' ')[1]
    month = date[:2]
    day = date[3:5]
    return day, month, time

def center_cityzen_to_location(location):
    # center 41.863976, -87.631273
    lat1 = location[0]
    lon1 = location[1]
    lat2 = 41.863976
    lon2 = -87.631273
        # Use the Haversine formula to calculate the distance in meters between two points
    geodesic_distance = great_circle((lat1, lon1), (lat2, lon2)).m

    # Assuming you're working with a specific reference latitude and longitude (lat_ref, lon_ref)
    # Calculate XY coordinates
    lat_ref = 0  # Reference latitude in radians
    lon_ref = 0  # Reference longitude in radians

    # Radius of the Earth (mean value) in meters
    R = 6371000

    x = R * (lon2 - lon_ref)
    y = R * (lat2 - lat_ref)

    return geodesic_distance






#---------------------------------------------------Main-------------------------------------------------
#---------------------------------------------------Gen_Dim-------------------------------------------------

##############################################Run_1_time_to_gen_dimentions######################################

df_crime = pd.read_csv('Crimes_-_2022 (1).csv', header=0, low_memory=False)

# Vamos dropar as colunas X Coordinate, Y Coordinate, Latitude e Longitude
df_crime = df_crime.drop(['Latitude', 'Longitude', 'X Coordinate', 'Y Coordinate', 'Year'], axis=1)
# Limpar a coluna Location pois estava vindo com NaN
df_crime = df_crime.dropna(subset=['Location', 'Ward'], how='any')


df_date = df_crime[['Date']]
df_date = df_date.loc[:, ['Date']].copy()
df_date['Period'] = df_date['Date'].apply(time_to_period)
df_date['Weekend'] = df_date['Date'].apply(is_weekend)
df_date['Holiday'] = df_date['Date'].apply(is_holiday)
df_date['Month'] = df_date['Date'].apply(lambda x: split_date_time(x)[1])
df_date['Day'] = df_date['Date'].apply(lambda x: split_date_time(x)[0])
df_date['Hour of the day'] = df_date['Date'].apply( lambda x: split_date_time(x)[2])
df_date['id_date'] = df_date.index + 1
df_date.to_csv('DIM_Date.csv')



df_location = df_crime[['Ward', 'District', 'Block', 'Beat', 'Community Area', 'Location Description', 'Location']]
df_location = df_location.copy()
df_location['Zone'] = df_location['Ward'].apply(ward_to_zone)
df_location['id_location'] = df_location.index + 1
df_location.to_csv('DIM_Location.csv')



df_location['is_active'] = True
df_location['start_date'] = datetime.datetime.now()
df_location['end_date'] = datetime.datetime(9999, 12, 31)


df_StatusCrime = df_crime[['Arrest', 'Domestic']]
df_StatusCrime['id_StatusCrime'] = df_StatusCrime.index + 1
df_StatusCrime.to_csv('DIM_StatusCrime.csv')



df_CrimeType = df_crime[['IUCR','Primary Type','Description', 'FBI Code']]
df_CrimeType['id_CrimeType'] = df_CrimeType.index + 1
df_CrimeType.to_csv('DIM_CrimeType.csv')

df_fact = df_crime[['ID','Case Number','Updated On']]
df_fact = pd.concat([df_fact,df_StatusCrime['id_StatusCrime']],axis=1)
df_fact = pd.concat([df_fact,df_location['id_location']],axis=1)
df_fact = pd.concat([df_fact,df_date['id_date']],axis=1)
df_fact = pd.concat([df_fact,df_CrimeType['id_CrimeType']],axis=1)
df_fact['avg_distance_to_city_center'] = df_crime['Location'].apply(center_cityzen_to_location)
df_fact.to_csv('DIM_fato.csv')

#-------------------------------------------------Loading_Data-------------------------------------------------
##########################################Use_alredy_created_DIM##########################################

# Define the column names
# df_crime = pd.read_csv('Crimes_-_2022.csv', header=0, low_memory=False)
# df_date = pd.read_csv('DIM_Date.csv', header=0, low_memory=False)
# df_location = pd.read_csv('DIM_Location.csv', header=0, low_memory=False)
# df_StatusCrime = pd.read_csv('DIM_StatusCrime.csv', header=0, low_memory=False)
# df_CrimeType = pd.read_csv('DIM_CrimeType.csv', header=0, low_memory=False)


# column_names = ['Case Number', 'Updated On', 'Total Occurrences', 'Total Type Occurrences', 'Average Occurrences', 'Max Occurrences', 'Average Distance to City Center']

# # Create an empty DataFrame with specified column names
# dimfato = pd.DataFrame(columns=column_names)

# dimfato.insert(0, 'ID', df_crime['ID'])

# fato = dimfato['ID'].copy()

# fato = pd.DataFrame(fato)


# dim_crime = pd.DataFrame(df_crime)
# dim_date = pd.DataFrame(df_date)
# dim_location = pd.DataFrame(df_location)
# dim_StatusCrime = pd.DataFrame(df_StatusCrime)
# dim_CrimeType = pd.DataFrame(df_CrimeType)


# fato['id_date'] = fato['ID'].map(dim_crime.set_index('ID')['Date'].reset_index().merge(dim_date, on='Date').set_index('ID')['id_date'])
# fato['id_CrimeType'] = fato['ID'].map(dim_crime.set_index('ID')['IUCR'].reset_index().merge(dim_CrimeType, on='IUCR').set_index('ID')['id_CrimeType'])
# fato['id_StatusCrime'] = fato['ID'].map(dim_crime.set_index('ID')['Arrest'].reset_index().merge(dim_StatusCrime, on='Arrest').set_index('ID')['id_StatusCrime'])
# fato['id_location'] = fato['ID'].map(dim_crime.set_index('ID')['Ward'].reset_index().merge(dim_location, on='Ward').set_index('ID')['id_location'])

# fato.to_csv('fact.csv')



# drive.mount('drive')

# dim_crime.to_csv('dim_crime.csv')
# !cp dim_crime.csv "drive/My Drive/"

# dim_date.to_csv('dim_date.csv')
# !cp dim_date.csv "drive/My Drive/"

# dim_location.to_csv('dim_location.csv')
# !cp dim_location.csv "drive/My Drive/"

# dim_StatusCrime.to_csv('dim_StatusCrime.csv')
# !cp dim_StatusCrime.csv "drive/My Drive/"

# dim_CrimeType.to_csv('dim_CrimeType.csv')
# !cp dim_CrimeType.csv "drive/My Drive/"
