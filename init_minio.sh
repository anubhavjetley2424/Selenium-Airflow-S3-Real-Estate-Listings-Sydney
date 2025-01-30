#!/bin/sh
# รอจนกว่า MinIO จะพร้อมทำงาน
until (/usr/bin/mc config host add minio http://minio:9000 admin password); do 
    echo '...waiting for MinIO to be ready...' 
    sleep 1 
done

# สร้าง Bucket 'rawdata' (หากไม่มีอยู่แล้ว)
if /usr/bin/mc ls minio/rawdata; then
    echo 'Bucket rawdata already exists. Skipping creation.'
else
    echo 'Creating bucket rawdata...'
    /usr/bin/mc mb minio/rawdata
    /usr/bin/mc policy set public minio/rawdata
    echo 'Bucket rawdata created and set to public.'
fi

# รอให้ MinIO ทำงานต่อ
tail -f /dev/null
