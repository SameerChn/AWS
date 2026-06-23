# AWS Sales ETL Pipeline

### S3 → Lambda → Glue → Parquet

An automated ETL pipeline that triggers on CSV upload, cleans the data using PySpark on AWS Glue, and stores the result as Parquet files back in S3.

---

## Architecture Overview

```
sales_raw.csv
      │
      ▼
+----------------+
|  Amazon S3     |  ← raw/ folder (upload trigger)
+----------------+
      │
      │  PUT Event
      ▼
+----------------+
|  AWS Lambda    |  ← Python 3.12
+----------------+
      │
      │  Start Glue Job
      ▼
+----------------+
|  AWS Glue      |  ← PySpark ETL
+----------------+
      │
      ▼
+----------------+
|  Amazon S3     |  ← processed/ folder (Parquet output)
+----------------+
      │
      ▼
+----------------+
|  CloudWatch    |  ← Lambda + Glue logs
+----------------+
```

---

## Prerequisites

- An active AWS account
- IAM permissions to create S3 buckets, IAM roles, Lambda functions, and Glue jobs

---

## Step 1: Create an S3 Bucket

1. Go to the **AWS Console** → search for **S3**
2. Click **Create Bucket**
3. Give it a unique name, e.g., `sales-etl-project`
4. Leave all other settings as default
5. Click **Create bucket**

Inside the bucket, create two folders:

```
sales-etl-project/
├── raw/
└── processed/
```

> **How to create folders:** Open the bucket → Click **Create folder** → Name it `raw` → Repeat for `processed`

---

## Step 2: Prepare the Sample CSV

Create a file named `sales_raw.csv` locally with this content:

```csv
order_id,customer_name,email,product,quantity,price,order_date
1,Rahul,rahul@gmail.com,Laptop,2,50000,2025-01-01
2,Amit,amit@gmail.com,Phone,1,20000,2025-01-02
3,,abc@gmail.com,Mouse,3,500,2025-01-03
1,Rahul,rahul@gmail.com,Laptop,2,50000,2025-01-01
```

> **Do not upload yet.** This file will be uploaded in Step 10 to trigger the pipeline.

The data intentionally contains:

- A **duplicate row** (row 1 and row 4 are identical)
- A **missing customer name** (row 3)

These will be cleaned by the Glue job.

---

## Step 3: Create IAM Role for Glue

1. Go to **IAM** → **Roles** → **Create role**
2. **Trusted entity type:** AWS service
3. **Use case:** Glue
4. Attach the following policies:
   - `AWSGlueServiceRole`
   - `AmazonS3FullAccess`
   - `CloudWatchLogsFullAccess`
5. Name the role: `GlueETLRole`
6. Click **Create role**

---

## Step 4: Create IAM Role for Lambda

1. Go to **IAM** → **Roles** → **Create role**
2. **Trusted entity type:** AWS service
3. **Use case:** Lambda
4. Attach the following policies:
   - `AWSLambdaBasicExecutionRole`
   - `AWSGlueConsoleFullAccess`
   - `AmazonS3ReadOnlyAccess`
5. Name the role: `LambdaGlueRole`
6. Click **Create role**

---

## Step 5: Create the Glue Job

1. Go to **AWS Glue** → **ETL Jobs**
2. Click **Create job**
3. Configure:
   - **Name:** `SalesETL`
   - **IAM Role:** `GlueETLRole`
   - **Type:** Spark
   - **Language:** Python (PySpark)

---

## Step 6: Write the Glue PySpark Script

Replace the default script with the following. Make sure to replace `YOUR_BUCKET` with your actual bucket name (e.g., `sales-etl-project`).

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read raw CSV from S3
df = spark.read.csv(
    "s3://YOUR_BUCKET/raw/",
    header=True,
    inferSchema=True
)

# Remove duplicate rows
df = df.dropDuplicates()

# Remove rows with null customer_name or quantity
df = df.filter(col("customer_name").isNotNull())
df = df.filter(col("quantity").isNotNull())

# Cast columns to correct types
df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.withColumn("price", col("price").cast("int"))

# Add derived column
df = df.withColumn("total_amount", col("quantity") * col("price"))

# Parse date column
df = df.withColumn("order_date", to_date(col("order_date")))

# Write cleaned data as Parquet to processed/ folder
df.write.mode("overwrite").parquet(
    "s3://YOUR_BUCKET/processed/"
)
```

**What this script does:**

| Step   | Transformation                                          |
| ------ | ------------------------------------------------------- |
| Read   | Load CSV from `raw/`                                    |
| Clean  | Drop duplicate rows                                     |
| Clean  | Filter out rows with null `customer_name` or `quantity` |
| Cast   | Convert `quantity` and `price` to integers              |
| Derive | Create `total_amount = quantity × price`                |
| Parse  | Convert `order_date` string to date type                |
| Write  | Save as Parquet to `processed/`                         |

Save the job after pasting the script.

---

## Step 7: Create the Lambda Function

1. Go to **Lambda** → **Create function**
2. Select **Author from scratch**
3. Configure:
   - **Function name:** `StartGlueJob`
   - **Runtime:** Python 3.12
   - **Execution role:** Use an existing role → `LambdaGlueRole`
4. Click **Create function**

---

## Step 8: Add Lambda Code

Replace the default handler code with:

```python
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    print(f"File uploaded: {file_name}")

    response = glue.start_job_run(JobName="SalesETL")

    return {
        "statusCode": 200
    }
```

Click **Deploy** to save.

---

## Step 9: Configure the S3 Trigger

1. Inside the `StartGlueJob` Lambda function, click **Add trigger**
2. Select **S3**
3. Configure:
   - **Bucket:** `sales-etl-project`
   - **Event type:** PUT
   - **Prefix:** `raw/`
4. Acknowledge the recursive invocation warning
5. Click **Add**

> From this point, any file uploaded to `raw/` will automatically trigger this Lambda.

---

## Step 10: Fix Lambda Permissions (If Needed)

If Lambda cannot start the Glue job, manually attach the required policy:

1. Go to **Lambda** → open `StartGlueJob`
2. Go to **Configuration** → **Permissions**
3. Click the execution role link (e.g., `StartGlueJob-role-xxxxxxxx`)
4. In IAM, click **Add permissions** → **Attach policies**
5. Attach one of:
   - `AWSGlueConsoleFullAccess` ← simpler, good for learning
   - Or a custom policy with only `glue:StartJobRun` ← better for production

---

## Step 11: Upload the CSV to Trigger the Pipeline

Upload `sales_raw.csv` into the `raw/` folder in your S3 bucket.

**Flow that happens automatically:**

```
CSV Uploaded to raw/
        ↓
S3 sends PUT Event
        ↓
Lambda receives the event
        ↓
Lambda calls glue.start_job_run("SalesETL")
        ↓
Glue reads, cleans, and writes Parquet to processed/
```

No manual intervention required after the upload.

---

## Step 12: Verify the Output

1. Go to **S3** → `sales-etl-project` → `processed/`
2. You should see files like:
   ```
   part-00000-xxxxxxxx.snappy.parquet
   _SUCCESS
   ```

The presence of `_SUCCESS` confirms the Glue job completed without errors.

---

## Step 13: Check Logs in CloudWatch

1. Go to **CloudWatch** → **Log groups**
2. Find:
   - `/aws/lambda/StartGlueJob` — Lambda execution logs
   - `/aws-glue/jobs/output` — Glue job logs

Use these logs to debug if anything fails.

---

## File Structure Reference

```
sales-etl-project/          ← S3 Bucket
├── raw/
│   └── sales_raw.csv       ← Input (triggers pipeline)
└── processed/
    ├── part-00000.snappy.parquet   ← Cleaned output
    └── _SUCCESS
```

---

## IAM Roles Summary

| Role             | Trusted Entity | Policies                                                                            |
| ---------------- | -------------- | ----------------------------------------------------------------------------------- |
| `GlueETLRole`    | AWS Glue       | `AWSGlueServiceRole`, `AmazonS3FullAccess`, `CloudWatchLogsFullAccess`              |
| `LambdaGlueRole` | AWS Lambda     | `AWSLambdaBasicExecutionRole`, `AWSGlueConsoleFullAccess`, `AmazonS3ReadOnlyAccess` |

---

## Troubleshooting

| Issue                     | Check                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| Lambda not triggering     | Verify S3 trigger has correct prefix (`raw/`) and event type (`PUT`)   |
| Glue job fails to start   | Ensure Lambda role has `glue:StartJobRun` permission (Step 10)         |
| No output in `processed/` | Check Glue logs in CloudWatch for PySpark errors                       |
| Empty Parquet files       | All rows may have been filtered — check input CSV for nulls/duplicates |
| Permission denied on S3   | Verify `GlueETLRole` has `AmazonS3FullAccess` attached                 |

---

## Technologies Used

- **Amazon S3** — Storage for raw and processed data
- **AWS Lambda** — Serverless trigger to start the ETL
- **AWS Glue** — Managed PySpark ETL service
- **Amazon CloudWatch** — Logging and monitoring
- **Apache Parquet** — Columnar output format (efficient for analytics)
