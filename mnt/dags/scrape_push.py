from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def scrape_real_estate():
    # Setup the Chrome WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run Chrome in headless mode (no UI)
    options.add_argument("--no-sandbox")  # Required for running as root in containers
    options.add_argument("--disable-dev-shm-usage")  # Prevent /dev/shm issues in Docker
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    options.add_argument("--remote-debugging-port=9222")  # Prevent debugging issues
    options.add_argument("--user-data-dir=/tmp/chrome-user-data")  # Unique user data directory

# Create the WebDriver instance
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get("https://www.domain.com.au/sale/castle-hill-nsw-2154/house/?excludeunderoffer=1")
    time.sleep(5)  # Allow time for page to load

    # Extract property listings
    listings = driver.find_elements(By.CLASS_NAME, "css-1pmltjx")
    
    addresses, prices, features = [], [], []

    for listing in listings:
        try:
            address_element = listing.find_element(By.CLASS_NAME, 'css-bqbbuf')
            address = address_element.text.strip() if address_element else ""
            
            price_element = listing.find_element(By.CLASS_NAME, "css-mgq8yx")
            price = price_element.text.strip() if price_element else ""

            feature_element = listing.find_element(By.CLASS_NAME, "css-18biwo")
            feature = feature_element.text.strip() if feature_element else ""
            
            if address:
                addresses.append(address)
                prices.append(price)
                features.append(feature)

        except Exception as e:
            print(f"Error extracting listing: {e}")

    # Store in a DataFrame
    df = pd.DataFrame({"Address": addresses, "Market Value": prices, 'Features': features})
    df['Features'] = df['Features'].astype(str)

    # Splitting the 'Features' column into multiple extracted columns
    df[['Bedrooms', 'Bathrooms', 'Parking Spaces', 'Property Area']] = df['Features'].apply(lambda x: pd.Series([
        x.split("\n")[0],
        x.split("\n")[2],
        x.split("\n")[4],
        x.split("\n")[6] if len(x.split("\n")) == 7 else '0m²'
    ]))

    df[['Bedrooms', 'Bathrooms', 'Parking Spaces']] = df[['Bedrooms', 'Bathrooms', 'Parking Spaces']].astype(int)
    df = df[['Address', 'Market Value', 'Bedrooms', 'Bathrooms', 'Parking Spaces', 'Property Area']]
    # Save to CSV
    file_path = "data/real_estate_listings.csv"
    df.to_csv(file_path, index=False)
    
    driver.quit()

def upload_to_s3():
    file_path = "data/real_estate_listings.csv"
    bucket_name = "castle-hill-realestate-listings"
    s3 = boto3.client('s3', aws_access_key_id='#access_id', aws_secret_access_key='#secret_key')
    s3.upload_file(file_path, bucket_name, "real_estate_listings.csv")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 20),
    'retries': 1
}

dag = DAG(
    'real_estate_scraper',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

scrape_task = PythonOperator(
    task_id='scrape_real_estate_data',
    python_callable=scrape_real_estate,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

scrape_task >> upload_task