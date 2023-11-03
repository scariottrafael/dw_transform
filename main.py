#----------------------------------------------Libs------------------------------------------------------

import numpy as np
import csv
import pandas as pd
import tables
import psycopg2
import re
import holidays
import datetime
#---------------------------------------------------Functions-------------------------------------------------
def handle_scd_type2(source_df, target_df, attributes):
    # Create a copy of the source DataFrame to avoid modifying the original data
    final_df = source_df.copy()

    # Create a list to store new rows
    new_rows = []

    # Iterate over the rows of the target DataFrame
    for index, row in target_df.iterrows():
        # If the index is in the source DataFrame, check if any attribute has changed
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



#---------------------------------------------------Main-------------------------------------------------
#---------------------------------------------------Gen_Dim-------------------------------------------------

df_crime = pd.read_csv('Crimes_-_2022.csv', header=0, low_memory=False)

# Vamos dropar as colunas X Coordinate, Y Coordinate, Latitude e Longitude
df_crime = df_crime.drop(['Latitude', 'Longitude', 'X Coordinate', 'Y Coordinate', 'Year'], axis=1)
# Limpar a coluna Location pois estava vindo com NaN
df_crime = df_crime.dropna(subset=['Location', 'Ward'], how='any')

print('passei1')
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

print('passei2')

df_location = df_crime[['Ward', 'District', 'Block', 'Beat', 'Community Area', 'Location Description', 'Location']]
df_location = df_location.copy()
df_location['Zone'] = df_location['Ward'].apply(ward_to_zone)
df_location['id_location'] = df_location.index + 1
df_location.to_csv('DIM_Location.csv')


print('passei3')
df_location['is_active'] = True
df_location['start_date'] = datetime.datetime.now()
df_location['end_date'] = datetime.datetime(9999, 12, 31)

print('passei4')
df_StatusCrime = df_crime[['Arrest', 'Domestic']]
df_StatusCrime['id_StatusCrime'] = df_StatusCrime.index + 1
df_StatusCrime.to_csv('DIM_StatusCrime.csv')


print('passei5')
df_CrimeType = df_crime[['IUCR','Primary Type','Description', 'FBI Code']]
df_CrimeType['id_CrimeType'] = df_CrimeType.index + 1
df_CrimeType.to_csv('DIM_CrimeType.csv')

#-------------------------------------------------Loading_Data-------------------------------------------------
# # Define the column names
# column_names = ['Case Number', 'Updated On', 'Total Occurrences', 'Total Type Occurrences', 'Average Occurrences', 'Max Occurrences', 'Average Distance to City Center']

# # Create an empty DataFrame with specified column names
# dimfato = pd.DataFrame(columns=column_names)

# dimfato.insert(0, 'ID', df_crime['ID'])

# fato = dimfato['ID'].copy()

# fato = pd.DataFrame(fato)

# merged_df = fato.merge(df_crime, on =['ID'])

# final_df = merged_df.merge(df_date, on='Date')
# fato['id_date'] = final_df['id_date']

# final_df = merged_df.merge(df_CrimeType, on='IUCR')
# fato['id_CrimeType'] = final_df['id_CrimeType']

# final_df = merged_df.merge(df_StatusCrime, on='Arrest')
# fato['id_StatusCrime'] = final_df['id_StatusCrime']

# final_df = merged_df.merge(df_location, on='Ward')
# fato['id_location'] = final_df['id_location']

# fato = dimfato['ID'].copy()

# fato = pd.DataFrame(fato)

# # Assuming df_fact is your fact table and df_dim is your dimension table
# # 'key' is the common column in both dataframes

# merged_df = pd.merge(fato, df_date, how='left', left_on='id_date', right_on='id_date')
