import pandas as pd
import datetime
import holidays




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

def is_holiday(day_str):
    us_holidays =  holidays.country_holidays('US')
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

    satats = date_day in us_holidays
    return satats

def time_to_shift(date_str):
    horas, min, seg = int, int, int
    period = str
    datetime_split = date_str.split(' ')
    partes_data = datetime_split[1].split(':')


    if len(partes_data) != 3:
        print("Formato de tempo inválido. Use o formato hora:min:seg.")
        return None

    try:
        horas = int(partes_data[0])
        min = int(partes_data[1])
        seg = int(partes_data[2])
    except ValueError:
        print("Formato de tempo inválido. Certifique-se de que são números inteiros.")

    if datetime_split[2]=='AM':
        if 6 < horas <= 12:
            period = "morning"
        elif 0 < horas <= 6:
            period = "dawn"

    elif datetime_split[2]=='PM':
        if 6 < horas <= 12:
            period = "night"
        elif 0 < horas <= 6:
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
        print('Zona não registrada no catalogo de zonas' )
    
    return zone

