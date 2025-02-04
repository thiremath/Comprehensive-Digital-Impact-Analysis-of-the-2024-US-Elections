import requests
import time
from requests.auth import HTTPBasicAuth
import logging
from pyfaktory import Client, Consumer, Job, Producer
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import datetime
import sys
import os
from dotenv import load_dotenv
import json

# Setup logging
logger = logging.getLogger("RedditCrawler")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# Load environment variables
load_dotenv()

# Environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
FAKTORY_SERVER_URL = os.getenv('FAKTORY_SERVER_URL')

# Reddit API credentials
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
USER_AGENT = os.getenv('USER_AGENT')

#Get board name
# SUBREDDIT = os.getenv('SUBREDDIT')

# Reddit's OAuth2 token URL
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
# Register the adapter for psycopg2 to insert dicts into a jsonb column
register_adapter(dict, Json)


# Add existing functions for token fetching, subreddit data fetching, storing in database, etc.

def cold_start_reddit_crawl(subreddit):
    """Cold start the Reddit crawler by scheduling the first job."""
    logger.info(f"Cold starting crawl for subreddit {subreddit}")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        job = Job(jobtype="crawl-subreddit", args=(subreddit,), queue="crawl-subreddit")
        producer.push(job)


def load_boards_from_json():
    with open('config.json', 'r') as f:
        data = json.load(f)
        return data.get('boards', [])


if __name__ == "__main__":
    # if len(sys.argv) < 2:
    #     print("Usage: python reddit_crawler.py <subreddit>")
    #     sys.exit(1)

    # subreddit = sys.argv[1]

    # Load subreddit name
    SUBREDDIT = load_boards_from_json()  # Update with your JSON file path

    if not SUBREDDIT:
        logger.error("No subreddit specified in .env file.")
        sys.exit(1)

    print(f"Cold starting crawl for subreddit {SUBREDDIT}")
   
    # Cold start the crawl
    logger.info(f"Cold starting crawl for subreddit {SUBREDDIT}")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        job = Job(jobtype="crawl-subreddit", args=(SUBREDDIT,), queue="crawl-subreddit")
        producer.push(job)
