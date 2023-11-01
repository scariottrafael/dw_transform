import pandas as pd
import holidays
import datetime

df = pd.read_csv('data.csv')
# Drop the Columns Latitude, Longitude, X Coordinate and Y Coordinate
df = df.drop(['Latitude', 'Longitude', 'X Coordinate', 'Y Coordinate'], axis=1)
# Clean the dataset
df_cleaned = df.dropna(subset=['Location'], how='any')
# Print the lenght of the dataset
print(len(df_cleaned))
print(df_cleaned.head())
# Export the dataset to a new csv file
df_cleaned.to_csv('data_cleaned.csv', index=False)

def split_date_time(date_time):
    date = date_time.split(' ')[0]
    time = date_time.split(' ')[1]
    month = date[:2]
    day = date[3:5]
    return day, month, time

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
        print('Zona não registrada no catalogo de zonas' )
    
    return zone

def is_weekend(day_str):
    status = bool
    dia, mes, ano = int, int, int
    datetimme_splitv = day_str.split(' ')
    partes_data = datetimme_splitv[0].split('/')

    if len(partes_data) != 3:
        print("Formato de data inválido. Use o formato dia/mês/ano.")
        return None

    try:
        dia = int(partes_data[0])
        mes = int(partes_data[1])
        ano = int(partes_data[2])
    except ValueError:
        print("Formato de data inválido. Certifique-se de que são números inteiros.")

    date_day = datetime.date(ano, dia, mes)

    if date_day.weekday() != 5 or date_day.weekday() != 6:
        status = False
    elif date_day.weekday() == 5 or date_day.weekday() == 6:
        status = True 
    return status


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
            if 6 <= horas <= 12:
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
def is_holiday(day_str):
    us_holidays = holidays.US()
    datetimme_splitv = day_str.split(' ')
    partes_data = datetimme_splitv[0].split('/')
    # Build a string with the date in the format YYYY-MM-DD, the partes_data[2] is the year, partes_data[0] is the month and partes_data[1] is the day
    date_day = str(partes_data[2]) + '-' + str(partes_data[0]) + '-' + str(partes_data[1])
    is_holiday = date_day in us_holidays

    return is_holiday

# Create a function to create the fact table and the dimension tables
def create_tables():
    # Create the fact table
    df_fact = df_cleaned[['Date', 'Location']]
    df_fact.to_csv('fact_table.csv', index=False)
    # Create the dimension table for the date
    df_date = df_cleaned[['Date', 'Year']]
    # Create a new column for the day, month, year and time
    df_date = df_date.loc[:, ['Date', 'Year']].copy()
    # df_date['Day'] = df_date['Date'].apply(lambda x: split_date_time(x)[0])
    # df_date['Month'] = df_date['Date'].apply(lambda x: split_date_time(x)[1])
    # df_date['Time'] = df_date['Date'].apply( lambda x: split_date_time(x)[3])
    df_date['Shift'] = df_date['Date'].apply(time_to_period)
    df_date['Holiday'] = df_date['Date'].apply(is_holiday)
    df_date.to_csv('dim_date.csv', index=False)
    # Create the dimension table for the location
    df_location = df_cleaned[['Block', 'Location Description', 'Beat', 'District', 'Ward', 'Community Area', 'Location']]
    df_location.to_csv('dim_location.csv', index=False)
    
create_tables()

# Create a function that receives a string containing a date and a time, the first two letters are the month, the next two are the day, and the last four are the year we have a space and then the time in the format HH:MM:SS
# Return a column for the day, month, year and one for the time

    