import requests
import json
import psycopg
import pandas as pd
from datetime import date


API_KEY = '19e607bc0fd305b5915dbd06cb3a05ef'


def getTickers(apiKey, dbname, user, password, port):
    #Для того, чтобы получить данные по всем тикерам, нужно создать цикл, который будет двигать
    #offset на limit, до того момента пока response['data'] не окажется пустой
    #Делаю без цикла, получаю первые 1000 записей т.к кол-во запросов ограничено
    url = f"https://api.marketstack.com/v1/tickers?access_key={apiKey}&limit=1000&offset=0"
    response = requests.get(url).json()

    with psycopg.connect(f"dbname={dbname} user={user} password={password} port={port}") as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.tickers")
            for item in response['data']:
                params = (json.dumps(item), )
                cur.execute("INSERT INTO stg.tickers(data) VALUES (%s)", params)


def getEndOfDayData(apiKey, dbname, user, password, port, lim):
    with psycopg.connect(f"dbname={dbname} user={user} password={password} port={port}") as conn:
        #Получаем тикеры из таблицы stg.tickers из Нью Йорка, 
        #Для простоты в дальнейшем получим данные за сентябрь только по ним
        #Так как запрос по одному тикеру ест 1 запрос, ввел lim, который ограничивает
        #количество итераций по тикерам из Нью Йорка
        NewYorkTickers = pd.read_sql('''select distinct "data"->> 'symbol' as "symbol"
                                        from stg.tickers
                                        where "data"-> 'stock_exchange' ->> 'city' = 'New York';''', conn)
        NewYorkTickersList = NewYorkTickers['symbol'].to_list()
        counter = 0
        while counter < lim:
            NewYorkTicker = NewYorkTickersList[counter]
            url = f'https://api.marketstack.com/v1/eod?access_key={apiKey}&symbols={NewYorkTicker}&limit=1000&date_from={date(2024,9,1)}&date_to={date(2024,9,30)}'
            response = requests.get(url).json()
            print(response)
            with conn.cursor() as cur:
                for item in response['data']:
                    params = (json.dumps(item), )
                    cur.execute("INSERT INTO stg.end_of_day_data(data) VALUES (%s)", params)
            counter += 1

#getEndOfDayData(API_KEY, 'postgres', 'user', 'user', 5431, 30)
#getTickers(API_KEY, 'postgres', 'user', 'user', 5431)
