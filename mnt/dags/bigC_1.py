from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import urllib
import json
from airflow.models import Variable
import os
import time
import psycopg2
from psycopg2 import sql
from minio import Minio
import re

# Define the default arguments
default_args = {
    'owner': 'norasit.k',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'email_on_failure': False,
    'email_on_retry': False,
}

# URLs to scrape
urls = {
    "fruit": "https://www.bigc.co.th/category/fruit?page=",
    "vegetable": "https://www.bigc.co.th/category/vegetables?page=",
    "meat": "https://www.bigc.co.th/category/meat?page="
}

def scrape_AndWriteFile(url, key_name, **kwargs):
    # กำหนด path สำหรับเก็บไฟล์ JSON
    current_dir = os.path.dirname(os.path.abspath(__file__))
    directory = os.path.join(current_dir, 'raw_files')
    os.makedirs(directory, exist_ok=True)

    date_str = kwargs['execution_date'].strftime("%Y_%m_%d")
    filename = f"bigC_{key_name}_{date_str}.json"
    filepath = os.path.join(directory, filename)

    data = []
    page = 1

    while True:
        # ดึงข้อมูลจาก URL
        full_url = f"{url}{page}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
        }

        response = requests.get(full_url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to fetch page {page}, status code: {response.status_code}")
            break
        
        soup = BeautifulSoup(response.content, 'html.parser')

        # ดึงข้อมูลชื่อสินค้าและราคา
        product_names = soup.find_all('div', {'class': 'productCard_title__f1ohZ'})
        product_prices = soup.find_all('div', {'class': 'productCard_price__9T3J8'})

        # นับจำนวนสินค้าบนหน้านี้
        product_count = len(product_names)
        print(product_count)

        # หากไม่มีสินค้าในหน้านี้ให้หยุดการทำงาน
        if product_count == 0:
            print(f"No products found on page {page}. Stopping scrape.")
            break

        # เก็บข้อมูลสินค้า
        for name, price in zip(product_names, product_prices):
            data.append({
                "name": name.text.strip(),
                "price": price.text.strip()
            })

        print(f"Scraped page {page}, found {product_count} products.")
        page += 1

    # บันทึกข้อมูลลงไฟล์ JSON
    with open(filepath, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to {filepath}")

    # ส่งคืนข้อมูลที่ใช้สำหรับ XCom
    return {
        "filepath": filepath,
        "page_count": page - 1,
        "product_count": len(data)
    }

# Task to create the table
def create_table():
    connection = psycopg2.connect(
        dbname="my_db",
        user="postgres_user",
        password="postgres_password",
        host="postgres",
        port="5432"
    )
    cursor = connection.cursor()

    create_table_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS bigC (
            id SERIAL PRIMARY KEY,
            scrape_date DATE NOT NULL,
            category TEXT NOT NULL,
            product_name_raw TEXT NOT NULL,
            product_name_transform TEXT NOT NULL,
            pricing_type TEXT NOT NULL,
            price NUMERIC(10, 2) NOT NULL
        );
        """
    )

    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'bigC' has been created (if not exists).")
    except Exception as e:
        print(f"Error creating table: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def create_storage(bucket_name):
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def move_FileToBucket(bucket_name, object_name, **kwargs):
    ti = kwargs['ti']
    filepath_dict = ti.xcom_pull(task_ids=kwargs['task_id_scrape'])
    filepath = filepath_dict['filepath']

    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )
    try:
        client.fput_object(bucket_name, object_name, filepath)
    except Exception as e:
        print(f"File {filepath} uploaded to {bucket_name}/{object_name}")
        raise

    os.remove(filepath)

def transform_and_insert(bucket_name, object_name):
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )

    local_file = f"/tmp/{object_name}"
    client.fget_object(bucket_name, object_name, local_file)

    with open(local_file, 'r', encoding='utf-8') as file:
        data = json.load(file)

    transformed_data = []

    # Extract information from filename
    filename_parts = object_name.split("_")
    category = '_'.join(filename_parts[1:-3])
    scrape_date = datetime.strptime("_".join(filename_parts[-3:]).replace(".json", ""), "%Y_%m_%d").date()

    for item in data:
        price_text = item['price']
        price_cleaned = float(price_text.replace(',', '').replace('฿', '').split('/')[0])

        # ใช้ regex เพื่อตรวจสอบ pattern "/อะไรก็ได้กก"
        if re.search(r'/.*กก', price_text):
            pricing_type = 'per_kg'
        else:
            pricing_type = 'fixed'

        transformed_data.append(
            (
                scrape_date,
                category,
                item['name'],
                item['name'],
                pricing_type,
                price_cleaned
            )
        )

    connection = psycopg2.connect(
        dbname="my_db",
        user="postgres_user",
        password="postgres_password",
        host="postgres",
        port="5432"
    )
    cursor = connection.cursor()

    insert_query = sql.SQL(
        """
        INSERT INTO bigC (
            scrape_date, category, product_name_raw, product_name_transform, pricing_type, price
        ) VALUES (%s, %s, %s, %s, %s, %s);
        """
    )

    try:
        cursor.executemany(insert_query, transformed_data)
        connection.commit()
        print("Data inserted into table 'bigC'.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

dag_id = 'BigC_1'
with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='Dynamic scraping on BigC',
    schedule_interval=None,
    start_date=datetime(2023, 12, 10),
    catchup=False,
) as dag:

    # Define the tasks in DAG
    start = EmptyOperator(task_id='start')

    empty1 = EmptyOperator(task_id='empty1')

    empty2 = EmptyOperator(task_id='empty2')

    end = EmptyOperator(task_id='end')

    create_storage_task = PythonOperator(
        task_id='create_storage',
        python_callable=create_storage,
        op_kwargs={'bucket_name': 'raw-data'},
    )

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    start >> [create_storage_task, create_table_task] >> empty1

    for key, url in urls.items():
        scrape_task = PythonOperator(
            task_id=f'scrape_{key}',
            python_callable=scrape_AndWriteFile,
            provide_context=True,
            op_kwargs={
                'url': url,
                'key_name': key,
            },
        )

        move_task = PythonOperator(
            task_id=f'move_{key}_json_to_bucket',
            python_callable=move_FileToBucket,
            op_kwargs={
                'bucket_name': 'raw-data',
                'object_name': f"bigC/bigC_{key}_{{{{ execution_date.strftime('%Y_%m_%d') }}}}.json",
                'task_id_scrape': f'scrape_{key}',
            },
        )

        empty1 >> scrape_task >> move_task >> empty2

    for key, url in urls.items():
        transform_task = PythonOperator(
            task_id=f'transform_and_insert_{key}',
            python_callable=transform_and_insert,
            op_kwargs={
                'bucket_name': 'raw-data',
                'object_name': f"bigC/bigC_{key}_{{{{ execution_date.strftime('%Y_%m_%d') }}}}.json",
            },
        )

        empty2 >> transform_task >> end