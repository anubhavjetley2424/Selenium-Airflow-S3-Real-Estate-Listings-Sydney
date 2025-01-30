FROM apache/airflow:2.9.0-python3.9

# ใช้ root เพื่อเพิ่ม dependencies
USER root

# ติดตั้ง dependencies ที่จำเป็น
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg libgconf-2-4 libnss3 libxss1 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libu2f-udev libvulkan1 fonts-liberation \
    libpq-dev gcc && \
    apt-get clean

# ติดตั้ง Google Chrome (Stable) เวอร์ชันล่าสุด
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm google-chrome-stable_current_amd64.deb && \
    echo "Google Chrome installed: $(google-chrome --version)"

# ติดตั้ง ChromeDriver จากลิงก์ที่กำหนด
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip -d /usr/local/bin && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /usr/local/bin/chromedriver-linux64 && \
    rm chromedriver-linux64.zip && \
    echo "ChromeDriver installed: $(chromedriver --version)"

# คัดลอกและตั้งค่า permission สำหรับ init_minio.sh
COPY init_minio.sh /init_minio.sh
RUN chmod +x /init_minio.sh

# ยืนยันการติดตั้ง
RUN google-chrome --version && chromedriver --version

# กลับไปใช้ผู้ใช้ airflow
USER airflow

# อัปเดต setuptools และ wheel
RUN pip install --upgrade pip setuptools wheel

# ติดตั้ง dependencies จาก requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอกไฟล์ DAGS, Plugins, และ Tests
COPY ./mnt/dags /opt/airflow/dags
COPY ./mnt/plugins /opt/airflow/plugins
COPY ./mnt/tests /opt/airflow/tests
