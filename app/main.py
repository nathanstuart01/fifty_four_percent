import argparse
import json

import pendulum

from processor import process_historical_files, process_s3_event
from config import ENV_CONFIG


def lambda_handler(event, context):
    print("Lambda triggered with event:", json.dumps(event))
    process_s3_event(event)


def parse_args():
    parser = argparse.ArgumentParser(description="Run data flattening manually.")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    CONFIG = ENV_CONFIG
    bucket = CONFIG["S3_BUCKET"]

    if not args.start_date or not args.end_date:
        print("You must specify both --start-date and --end-date.")
        exit(1)
    print("Processing historical files...")
    start = pendulum.from_format(args.start_date, "YYYY-MM-DD")
    end = pendulum.from_format(args.end_date, "YYYY-MM-DD")
    process_historical_files(start, end, bucket)
