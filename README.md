# Pyspark-text-data-cleaning

### Intro
This is a project that displays how to clean data using pyspark on the Google Cloud Platform. The data was scraped from Reddit (https://www.reddit.com/r/thinkpad/) and stored in Could Storage. The text data was cleaned using Spark-nlp on Dataproc and inserted into BigQuery. 

This project uses the following GCP services:
  - Cloud Storage (Stores simulated source data (reddit_thinkpad.csv))
  - Dataproc (For running Pyspark (clean-data.py))
  - BigQuery (Stores cleaned data)

---

### GCP Architecture

![architecture](gcp_architecture.png)
---

### Dataproc cluster command
- Create cluster

```
gcloud dataproc clusters create CLUSTER_NAME \
  --region=REGION \
  --image-version=2.0 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-2 \
  --master-boot-disk-size=128GB \
  --worker-boot-disk-size=128GB \
  --num-workers=2 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --metadata 'PIP_PACKAGES=google-cloud-storage spark-nlp==2.7.2 google-cloud-bigquery' \
  --initialization-actions gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
  --properties spark:spark.serializer=org.apache.spark.serializer.KryoSerializer,spark:spark.driver.maxResultSize=0,spark:spark.kryoserializer.buffer.max=2000M,spark:spark.jars.packages=com.johnsnowlabs.nlp:spark-nlp_2.12:3.3.4
```

- Submit jobs

```
gcloud dataproc jobs submit pyspark PYTHON_FILE \
    --cluster=CLUSTER_NAME \
    --region=REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --driver-log-levels root=FATAL,com.example=INFO
```
    
- Delete cluster

`gcloud dataproc clusters delete CLUSTER_NAME --region=REGION`
