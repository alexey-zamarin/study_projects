#импорт библиотек
import telegram
import pandahouse as ph
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры соединения - нужны, чтобы подключиться к нужной схеме данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator',
              'user':'student',
              'password':'dpo_python_2020'}

# получим доступ к боту
my_token = '6677446544:AAG0OyibdrwvF2NZ5sNX8NDYy6zz4_YCZc4'
bot = telegram.Bot(token = my_token)

#укажем номер чата, куда необходимо отправить report
chat_id = -938659451

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-zamarin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 12, 17)}

# Интервал запуска DAG
schedule_interval = '50 10 * * *'

#создаем dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_zamarin_report_feed():
    
    #вытащим данные из новостной ленты
    @task()
    def extract_feed():
        query = """
            SELECT toDate(time) AS date,
                COUNT(DISTINCT user_id) AS DAU,
                sum(action = 'view') as views,
                sum(action = 'like') as likes,
                likes/views as ctr
            FROM simulator_20231113.feed_actions 
            WHERE toDate(time) between yesterday() - 6 and yesterday()
            GROUP BY date
            ORDER BY date
            """

        df = ph.read_clickhouse(query=query, connection=connection)        
        sns.set(rc={'figure.figsize':(13, 7)})
        sns.lineplot(data=df, x = 'date', y = 'DAU')
        plt.title('Динамика DAU за последние 7 дней')
        return df
    
    #напишем текст отчета 
    @task()
    def send_report_text(df):
        
        #скорректируем формат даты
        df['date'] = df['date'].dt.date
        
        # определяем наши метрики
        yesterday_date = df.at[6, 'date']
        yesterday_DAU = df.at[6, 'DAU']
        yesterday_views = df.at[6, 'views']
        yesterday_likes = df.at[6, 'likes']
        yesterday_ctr = df.at[6, 'ctr']
        
        #записываем сообщение, которое будем отправлять в чат
        report = f''' 
        Значения ключевых метрик по ленте новостей за вчерашний день - {yesterday_date}:

        1. DAU - {yesterday_DAU}
        2. Просмотры - {yesterday_views}
        3. Лайки - {yesterday_likes}
        4. CTR - {round(yesterday_ctr, 4)}
        '''
        bot.sendMessage(chat_id = chat_id, text = report)
    
    #отправим графики в чат
    @task()
    def send_charts(df):
        # зададим метрики, графики которых будем выводить
        metrics = ['DAU', 'views', 'likes', 'ctr']
        
        # создадим цикл для вывода графиков
        for metric in metrics:
            sns.set(rc={'figure.figsize':(13, 7)})
            sns.lineplot(data=df, x = 'date', y = metric)
            plt.title(f'Динамика {str.upper(metric)} за последние 7 дней')
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'test_plot.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    #выполним все функции
    df = extract_feed()
    send_report_text(df)
    send_charts(df)

#запускаем dag
dag_zamarin_report_feed = dag_zamarin_report_feed()