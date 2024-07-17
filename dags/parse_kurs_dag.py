from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def parse_kurs():
    url = "https://kurs.kz/"
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Ищем теги <script> содержащие данные
    script_content = ""
    for script in soup.find_all('script'):
        if 'var punkts' in script.text or 'var punktsFromInternet' in script.text:
            script_content += script.string

    if not script_content:
        print("Данные не найдены")
        return

    # Извлекаем JSON данные из тега <script>
    punkts_data = []
    punkts_match = re.search(r'var punkts = (\[.*?\]);', script_content, re.DOTALL)
    if punkts_match:
        punkts_data.extend(json.loads(punkts_match.group(1)))

    punkts_from_internet_match = re.search(r'var punktsFromInternet = (\[.*?\]);', script_content, re.DOTALL)
    if punkts_from_internet_match:
        punkts_data.extend(json.loads(punkts_from_internet_match.group(1)))

    if not punkts_data:
        print("Не удалось извлечь данные")
        return

    # Инициализация переменных для хранения максимальных и минимальных значений
    max_value = None
    min_value = None
    max_currency = None
    min_currency = None

    # Проходим по всем пунктам и ищем максимальные и минимальные значения
    for punkt in punkts_data:
        for currency, values in punkt["data"].items():
            for value in values:
                if value != 0:
                    if max_value is None or value > max_value:
                        max_value = value
                        max_currency = currency
                    if min_value is None or value < min_value:
                        min_value = value
                        min_currency = currency

    print(f"Max value: {max_value} ({max_currency})")
    print(f"Min value: {min_value} ({min_currency})")

dag = DAG(
    'parse_kurs_dag',
    default_args=default_args,
    description='DAG для парсинга курса валют',
    schedule_interval=timedelta(minutes=20),
    start_date=datetime(2024, 7, 17),
    catchup=False,
)

run_parse_kurs = PythonOperator(
    task_id='run_parse_kurs',
    python_callable=parse_kurs,
    dag=dag,
)

run_parse_kurs
