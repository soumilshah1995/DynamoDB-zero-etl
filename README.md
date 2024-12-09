# DynamoDB-zero-etl
DynamoDB-zero-etl


# DynamoDB Zero-ETL Integration with Amazon Glue and Athena

This guide outlines the steps to set up a DynamoDB table, enable point-in-time recovery (PITR), create necessary IAM policies and roles, and configure Zero-ETL integration with Amazon Glue. This allows you to query DynamoDB data seamlessly in Athena.

## Prerequisites

- AWS CLI installed and configured
- AWS account with appropriate permissions

## Steps

### 1. Create a DynamoDB Table

```bash
aws dynamodb create-table \
    --table-name user \
    --attribute-definitions AttributeName=user_id,AttributeType=S \
    --key-schema AttributeName=user_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --region us-east-1
```


Insert Sample Data
```
aws dynamodb put-item \
    --table-name user \
    --item '{"user_id": {"S": "user1"}, "name": {"S": "Alice"}, "email": {"S": "alice@example.com"}}' \
    --region us-east-1

aws dynamodb put-item \
    --table-name user \
    --item '{"user_id": {"S": "user2"}, "name": {"S": "Bob"}, "email": {"S": "bob@example.com"}}' \
    --region us-east-1

aws dynamodb put-item \
    --table-name user \
    --item '{"user_id": {"S": "user3"}, "name": {"S": "Charlie"}, "email": {"S": "charlie@example.com"}}' \
    --region us-east-1

aws dynamodb put-item \
    --table-name user \
    --item '{"user_id": {"S": "user4"}, "name": {"S": "Soumil"}, "email": {"S": "soumil@example.com"}}' \
    --region us-east-1

```

Enable Point-in-Time Recovery (PITR)
```
aws dynamodb update-continuous-backups \
    --table-name user \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
    --region us-east-1

```

Create IAM Policies and Roles
```
aws iam create-policy \
    --policy-name DynamoDBZeroETLPolicy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:CreateInboundIntegration",
                    "glue:DeleteInboundIntegration",
                    "glue:GetInboundIntegration",
                    "glue:ListInboundIntegrations",
                    "glue:UpdateInboundIntegration"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:DescribeTable",
                    "dynamodb:PutResourcePolicy",
                    "dynamodb:GetResourcePolicy",
                    "dynamodb:DeleteResourcePolicy",
                    "dynamodb:ExportTableToPointInTime",
                    "dynamodb:DescribeExport",
                    "dynamodb:ListTables",
                    "dynamodb:GetShardIterator",
                    "dynamodb:DescribeStream",
                    "dynamodb:GetRecords",
                    "dynamodb:ListStreams"
                ],
                "Resource": "*"
            }
        ]
    }'

```

Create the GlueDatabaseAccessPolicy
```
aws iam create-policy \
    --policy-name FullGlueAccessToDefaultDB \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions"
                ],
                "Resource": [
                    "arn:aws:glue:us-east-1:<MASKED_ACCOUNT_ID>:catalog",
                    "arn:aws:glue:us-east-1:<MASKED_ACCOUNT_ID>:database/default",
                    "arn:aws:glue:us-east-1:<MASKED_ACCOUNT_ID>:table/default/*"
                ]
            }
        ]
    }'


```

Create the IAM Role for Glue
```
aws iam create-role --role-name MyGlueDynamoDBRole --assume-role-policy-document '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

```

Attach the Policies to the Role
```
aws iam attach-role-policy \
    --role-name MyGlueDynamoDBRole \
    --policy-arn arn:aws:iam::<MASKED_ACCOUNT_ID>:policy/DynamoDBZeroETLPolicy

aws iam attach-role-policy \
    --role-name MyGlueDynamoDBRole \
    --policy-arn arn:aws:iam::<MASKED_ACCOUNT_ID>:policy/GlueDatabaseAccessPolicy

```

Add Resource Policy for Zero-ETL Integration
```
aws dynamodb put-resource-policy \
    --resource-arn arn:aws:dynamodb:us-east-1:<MASKED_ACCOUNT_ID>:table/users \
    --policy '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "1111",
        "Effect": "Allow",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Action": [
          "dynamodb:ExportTableToPointInTime",
          "dynamodb:DescribeTable",
          "dynamodb:DescribeExport"
        ],
        "Resource": "*",
        "Condition": {
          "StringEquals": {
            "aws:SourceAccount": "<MASKED_ACCOUNT_ID>"
          },
          "StringLike": {
            "aws:SourceArn": "arn:aws:glue:us-east-1:<MASKED_ACCOUNT_ID>:integration:*"
          }
        }
      }
    ]
    }'

```

Verify Resource Policy
```
aws dynamodb get-resource-policy \
    --resource-arn arn:aws:dynamodb:us-east-1:<MASKED_ACCOUNT_ID>:table/user

```

# DuckDB Query 
```
import os
import boto3
import duckdb

print("All Modules are loaded")


class DuckDBConnector:
    def __init__(self):
        self.conn = None

    def connect_and_setup(self) -> duckdb.DuckDBPyConnection:
        self.conn = duckdb.connect()
        extensions = ['iceberg', 'aws', 'httpfs']
        for ext in extensions:
            self.conn.execute(f"INSTALL {ext};")
            self.conn.execute(f"LOAD {ext};")
        self.conn.execute("CALL load_aws_credentials();")
        return self.conn


def get_latest_metadata_file(bucket_name: str, iceberg_path: str) -> str:
    s3 = boto3.client('s3')

    path_without_bucket = iceberg_path.replace(f's3://{bucket_name}/', '')
    metadata_prefix = f"{path_without_bucket.strip('/')}/metadata/"

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        metadata_files = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].endswith('.metadata.json')
        ]

        if metadata_files:
            latest_metadata = sorted(metadata_files)[-1]
            return f"s3://{bucket_name}/{latest_metadata}"
        else:
            print("No metadata files found")
            return None

    except Exception as e:
        print(f"Error listing objects: {str(e)}")
        return None


def main(iceberg_path: str, BUCKET: str):
    bucket_name = BUCKET
    latest_metadata_path = get_latest_metadata_file(bucket_name, iceberg_path)

    if not latest_metadata_path:
        print("No metadata files found.")
        return

    print("Latest Metadata File:", latest_metadata_path)

    connector = DuckDBConnector()
    conn = connector.connect_and_setup()

    try:
        query = f"SELECT * FROM iceberg_scan('{latest_metadata_path}')"
        result = conn.execute(query).fetchall()

        print("\nQuery Results:")
        for row in result:
            print(row)
    except duckdb.Error as e:
        print(f"Error executing DuckDB query: {e}")


if __name__ == "__main__":
    BUCKET = "XX"
    parquet_path = "s3://XXX/warehosue/user/"
    main(parquet_path, BUCKET)
```

# Athena 
```
SELECT * FROM "default"."user" limit 10;
```


