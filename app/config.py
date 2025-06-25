import os

from dotenv import load_dotenv

load_dotenv()

SPORT = os.getenv("SPORT")
DATA_SOURCE = os.getenv("DATA_SOURCE")
ENV = os.getenv("ENV", "dev")

ENV_CONFIG = {
    "S3_BUCKET": f"{SPORT}-{DATA_SOURCE}-crawl-data",
}
