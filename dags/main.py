
import json
import pendulum
from airflow.decorators import dag, task
import requests
import psycopg
import pandas as pd
from datetime import date
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 11, 17, tz="UTC"),
)
def marketstack():

    API_KEY = Variable.get("API_KEY")
    pg_conn = BaseHook.get_connection("postgres_default")

    @task()
    def getTickers():
        #Для того, чтобы получить данные по всем тикерам, нужно создать цикл, который будет двигать
        #offset на limit, до того момента пока response['data'] не окажется пустой
        #Делаю без цикла, получаю первые 1000 записей т.к кол-во запросов ограничено
        url = f"https://api.marketstack.com/v1/tickers?access_key={API_KEY}&limit=1000&offset=0"
        response = requests.get(url).json()

        with psycopg.connect(f"dbname={pg_conn.schema} user={pg_conn.login} password={pg_conn.password} host={pg_conn.host} port={pg_conn.port}") as conn:
            with conn.cursor() as cur:
                for item in response['data']:
                    params = (json.dumps(item), )
                    cur.execute("INSERT INTO stg.tickers(data) VALUES (%s)", params)

    @task()
    def getEndOfDayData(lim):
        with psycopg.connect(f"dbname={pg_conn.schema} user={pg_conn.login} password={pg_conn.password} host={pg_conn.host} port={pg_conn.port}") as conn:
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
                url = f'https://api.marketstack.com/v1/eod?access_key={API_KEY}&symbols={NewYorkTicker}&limit=1000&date_from={date(2024,9,1)}&date_to={date(2024,9,30)}'
                response = requests.get(url).json()
                print(response)
                with conn.cursor() as cur:
                    for item in response['data']:
                        params = (json.dumps(item), )
                        cur.execute("INSERT INTO stg.end_of_day_data(data) VALUES (%s)", params)
                counter += 1

    hub_company_key_not_null = SQLValueCheckOperator(
                                            task_id='hub_company_key_not_null',
                                            sql='''SELECT COUNT(1) FROM stg.tickers WHERE "data"->> 'symbol' is null''',
                                            pass_value=0,  # Ожидаемое значение (нет NULL)
                                            conn_id='postgres_default',
                                        )
    hub_exchange_key_not_null = SQLValueCheckOperator(
                                            task_id='hub_exchange_key_not_null',
                                            sql='''SELECT COUNT(1) FROM stg.tickers WHERE "data"-> 'stock_exchange' ->> 'mic' is null''',
                                            pass_value=0,  # Ожидаемое значение (нет NULL)
                                            conn_id='postgres_default',
                                        )
    hub_stock_key_not_null = SQLValueCheckOperator(
                                            task_id='hub_stock_key_not_null',
                                            sql='''SELECT COUNT(1) FROM stg.end_of_day_data WHERE "data"->> 'symbol' is null''',
                                            pass_value=0,  # Ожидаемое значение (нет NULL)
                                            conn_id='postgres_default',
                                        )

    insertHubs = PostgresOperator(task_id='insertHubs', sql='sql/insertHubs.sql', postgres_conn_id='postgres_default')
    insertLinks = PostgresOperator(task_id='insertLinks', sql='sql/insertLinks.sql', postgres_conn_id='postgres_default')
    insertSattelits = PostgresOperator(task_id='insertSattelits', sql='sql/insertSattelits.sql', postgres_conn_id='postgres_default')

    getTickers() >> getEndOfDayData(1) >> [hub_company_key_not_null, hub_exchange_key_not_null, hub_stock_key_not_null] >> insertHubs >> insertLinks >> insertSattelits

marketstack()
