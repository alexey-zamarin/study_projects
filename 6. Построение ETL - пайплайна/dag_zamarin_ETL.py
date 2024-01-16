#импортируем библиотеки

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры соединения - нужны, чтобы подключиться к нужной схеме данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator',
              'user':'student',
              'password':'dpo_python_2020'}

#коннект с кликхаусом
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database':'test',
                   'user':'student-rw',
                   'password':'656e2b0c9c'}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-zamarin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 12, 13)}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

#создаем dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_zamarin_ETL():
    
    #вытащим данные из новостной ленты
    @task()
    
    def extract_feed():
        query = """ 
            SELECT toDate(time) AS event_date, gender, age, os, user_id,
                   sum(action = 'like') as likes,
                   sum(action = 'view') as views
            FROM simulator_20231113.feed_actions 
            WHERE toDate(time) = yesterday()
            GROUP BY event_date, gender, age, os, user_id
                """
        
        feed = ph.read_clickhouse(query=query, connection=connection)
        return feed
    
    #вытащим данные из сообщений
    @task()
    
    def extract_message():
        query = """
                SELECT *, today() - 1 AS event_date
                FROM
                        (SELECT gender, os, age, user_id, 
                                COUNT(*) as messages_sent,
                                COUNT (DISTINCT receiver_id) as users_sent
                         FROM simulator_20231113.message_actions
                         WHERE toDate(time) = yesterday() 
                         GROUP BY gender, os, age, user_id) AS sent_message

                FULL OUTER JOIN

                        (SELECT gender, os, age, receiver_id AS user_id,
                                COUNT(*) as messages_received,
                                COUNT (DISTINCT user_id) as users_received
                        FROM simulator_20231113.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY gender, os, age, user_id) AS receive_message 
                USING (user_id, os, gender, age)
                    """
        
        messages = ph.read_clickhouse(query=query, connection=connection)
        return messages
    
    #объединим таблицы
    @task()
    def merge_tables(feed, messages):
        
        df = feed.merge(messages, 
                how = 'outer', 
                on = ['os', 'gender', 'age', 'user_id', 'event_date'])
        return df
    
    #срез os
    @task()
    def get_df_os(df):
        
        os_group = df.groupby(['event_date', 'os']).agg(
            {'views' : 'sum', 
             'likes' : 'sum', 
             'messages_received' : 'sum', 
             'messages_sent' : 'sum', 
             'users_received' : 'sum',
             'users_sent' : 'sum'}).reset_index().rename({'os': 'dimension_value'}, axis =1)
        os_group['dimension'] = 'os'
        os_group = os_group[['event_date', 'dimension', 'dimension_value', 
                        'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        
        return os_group
    
    #срез gender
    @task()
    def get_df_gender(df):
        
        gender_group = df.groupby(['event_date', 'gender']).agg(
            {'views' : 'sum', 
             'likes' : 'sum', 
             'messages_received' : 'sum', 
             'messages_sent' : 'sum', 
             'users_received' : 'sum',
             'users_sent' : 'sum'}).reset_index().rename({'gender': 'dimension_value'}, axis =1)
        gender_group['dimension'] = 'gender'
        gender_group = gender_group[['event_date', 'dimension', 'dimension_value', 
                        'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return gender_group
    
    
    #срез age
    @task()
    def get_df_age(df):
        
        age_group = df.groupby(['event_date', 'age']).agg(
            {'views' : 'sum', 
             'likes' : 'sum', 
             'messages_received' : 'sum', 
             'messages_sent' : 'sum', 
             'users_received' : 'sum',
             'users_sent' : 'sum'}).reset_index().rename({'age': 'dimension_value'}, axis =1)
        age_group['dimension'] = 'age'
        age_group = age_group[['event_date', 'dimension', 'dimension_value', 
                        'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        
        return age_group

    #загрузка в clickhouse
    @task
    def load_to_clickhouse(df_os, df_gender, df_age):
        
        #обединяю срезы, задаю нужные типы
        df_final = pd.concat([df_os, df_gender, df_age])
        
        df_final = df_final.astype({
            'views': 'int64',
            'likes': 'int64',
            'messages_received': 'int64',
            'messages_sent': 'int64',
            'users_received': 'int64',
            'users_sent': 'int64'})
              
        #создаем таблицу
        query_table = '''
        CREATE TABLE IF NOT EXISTS test.dag_zamarin_table
        (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_received Int64,
            messages_sent Int64,
            users_received Int64,
            users_sent Int64
            )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''        
        
        #запрос создания новой таблицы
        ph.execute(query_table, connection=connection_test)        
        
        #функция работает путём конкатенации, т.е. она просто присоединит новые данные к низу таблички
        ph.to_clickhouse(df=df_final, table="dag_zamarin_table", index=False, connection=connection_test)
        

    # начинаем выполнение всех функций
    
    # выгружаем данные
    feed = extract_feed()
    messages = extract_message()
    
    # объединяем таблицы, считаем метрики
    df = merge_tables(feed, messages)
    df_os = get_df_os(df)
    df_gender = get_df_gender(df)
    df_age = get_df_age(df)
    
    # выгружаем в clickhouse
    load_to_clickhouse(df_os, df_gender, df_age)

# запускаем dag
dag_zamarin_ETL = dag_zamarin_ETL()