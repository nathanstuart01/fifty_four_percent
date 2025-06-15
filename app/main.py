import argparse
import json
import os
from datetime import datetime, timedelta
from app.processor import process_historical_files, process_s3_event


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

    if not args.start_date or not args.end_date:
        print("You must specify both --start-date and --end-date.")
        exit(1)
    print("Processing historical files...")
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    process_historical_files(start_date=start, end_date=end)
