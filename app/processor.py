from datetime import datetime, timedelta
import boto3
import json

from validators.mlb.mlb_data_model import MLBParsedData


def process_historical_files(start_date: datetime, end_date: datetime, bucket: str):
    print(f"Processing historical files from {start_date.date()} to {end_date.date()} in bucket {bucket}")
    s3 = boto3.client("s3")
    for single_date in (
        start_date + timedelta(n) for n in range((end_date - start_date).days + 1)
    ):
        key = f"{single_date.year}/{single_date.month:02}/{single_date.day:02}.json.gz"
        print(f"Processing file: s3://{bucket}/{key}")

        # try:
        #     response = s3.get_object(Bucket=bucket, Key=key)
        #     data = json.loads(response["Body"].read())
        #     flattened = validate_data(data)
        #     save_to_postgres(flattened)
        #     populate_calculated_tables()
        # except s3.exceptions.NoSuchKey:
        #     print(f"File not found for {key}")
        #     continue


def process_s3_event(event):
    records = event.get("Records", [])
    for record in records:
        s3_info = record["s3"]
        bucket = s3_info["bucket"]["name"]
        key = s3_info["object"]["key"]

        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
        valid_data = validate_data(data)
        save_to_postgres(valid_data)


def validate_data(data: dict) -> dict:
    validated_data = MLBParsedData.model_validate(data)
    return validated_data


def save_to_postgres(data: dict):
    # Your DB insert logic here
    pass


def populate_calculated_tables():
    # Your derived/OLAP table logic here
    pass
