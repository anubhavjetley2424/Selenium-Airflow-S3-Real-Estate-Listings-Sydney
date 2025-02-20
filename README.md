# Selenium - Airflow - S3 Data Pipeline Architecture

![image](https://github.com/user-attachments/assets/fee587b2-9531-4f1b-acc8-3e0fd779a600)


Web scraping from Domain.com, listing Sydney property real estate listings. The pipeline operates on a daily basis, and can be useful for projects requiring updated data on property. 
The url the Selenium driver works on can be adjusted to scrape any suburb required.

# Note:
Make sure to enter AWS key credentials within the s3 connection.

# Utilisation
```
docker compose up --build
```
Use ``` https //localhost:8080 ``` to access airflow UI, enter username ``` airflow ``` and password ``` airflow ```
