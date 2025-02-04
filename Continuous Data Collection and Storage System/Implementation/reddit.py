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
SUBREDDIT = os.getenv('SUBREDDIT')

# Reddit's OAuth2 token URL
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
# Register the adapter for psycopg2 to insert dicts into a jsonb column
register_adapter(dict, Json)

def get_reddit_token():
    """Obtain OAuth2 token from Reddit."""
    auth = HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
    headers = {'User-Agent': USER_AGENT}
   
    logger.info("Attempting to fetch Reddit token...")
    response = requests.post(TOKEN_URL, auth=auth, data=data, headers=headers)

    if response.status_code == 200:
        logger.info("Successfully obtained access token!")
        return response.json().get('access_token')
    else:
        logger.error(f"Failed to get token: {response.status_code}, {response.json()}")
        return None

def fetch_subreddit_data(subreddit, token):
    """Fetch subreddit data from Reddit."""
    headers = {'Authorization': f'bearer {token}', 'User-Agent': USER_AGENT}
    api_url = f"https://oauth.reddit.com/r/{subreddit}/comments"
    params = {'limit': 10}

    logger.info(f"Fetching data from subreddit: {subreddit}")
    response = requests.get(api_url, headers=headers, params=params)

    time.sleep(1)

    if response.status_code == 200 and response.text:
        logger.info(f"Fetched data from subreddit {subreddit} successfully!")
        return response.json()
    else:
        logger.error(f"Failed to fetch subreddit data: {response.status_code}, {response.json()}")
        return None

def store_in_database(subreddit_data):
    """Store fetched data in the PostgreSQL database."""
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    logger.info(f"Storing subreddit data in the database...")

    try:
        # Make sure subreddit_data follows the expected structure
        posts = subreddit_data.get('data', {}).get('children', [])

        # Loop through each post in the subreddit data
        for post in posts:
            post_data = post.get('data', {})
            print(post_data)
           
            # Extract necessary fields from the post data
            post_id = post_data.get("id")
            title = post_data.get("title")
            subreddit = post_data.get("subreddit")
            author = post_data.get("author")
            score = post_data.get("score", 0)
            created_utc = datetime.datetime.utcfromtimestamp(post_data.get("created_utc"))

            if not post_id:
                continue  # Skip if post_id is missing

            # Insert the post data into the database
            q = """
                INSERT INTO reddit_posts (post_id, title, subreddit, author, score, created_utc, data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (post_id) DO NOTHING
            """
            cur.execute(q, (
                post_id,
                title,
                subreddit,
                author,
                score,
                created_utc,
                psycopg2.extras.Json(post_data)  # Store full post data as JSON
            ))

        conn.commit()
        logger.info(f"Subreddit data stored successfully in the database.")
    except Exception as e:
        logger.error(f"Failed to store data in the database: {e}")
    finally:
        cur.close()
        conn.close()

def crawl_subreddit(subreddit):
    """Crawl the subreddit, fetch data, and store it in the database."""
    token = get_reddit_token()
    if token:
        subreddit_data = fetch_subreddit_data(subreddit, token)
        if subreddit_data:
            store_in_database(subreddit_data)
            # Schedule the next crawl as soon as the current one finishes
            schedule_subreddit_crawl(subreddit)
        else:
            logger.error(f"No subreddit data to store for {subreddit}.")
            schedule_subreddit_crawl(subreddit)  # Still schedule next crawl even if there's an error
    else:
        logger.error(f"Failed to get Reddit token. Skipping subreddit crawl for {subreddit}.")
        schedule_subreddit_crawl(subreddit)

def schedule_subreddit_crawl(subreddit):
    """Schedule subreddit crawl with minimal delay."""
    logger.info(f"Scheduling crawl for subreddit: {subreddit}")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)

        # Use timezone-aware datetime for UTC
        run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=1)  # 1-second delay
        run_at_iso = run_at.isoformat()

        job = Job(jobtype="crawl-subreddit", args=(subreddit,), queue="crawl-subreddit", at=str(run_at_iso))
        producer.push(job)

def continuous_schedule(subreddit):
    """Continuously schedule the subreddit crawl."""
    while True:
        # Schedule the next job with minimal delay between schedules (1 second or less)
        schedule_subreddit_crawl(subreddit)
        time.sleep(1)  # Adjust the sleep interval if you want near-continuous scheduling

def load_boards_from_json():
    with open('config.json', 'r') as f:
        data = json.load(f)
        return data.get('boards', [])

if __name__ == "__main__":
    # subreddit=sys.argv[1]
    # subreddit = "politics"

    SUBREDDIT = load_boards_from_json()

    if not SUBREDDIT:
        logger.error("No subreddit specified in .env file.")
        sys.exit(1)
   
    # Set up the Faktory consumer to handle jobs
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-subreddit"], concurrency=5)
       
        # Register job types
        consumer.register("crawl-subreddit", crawl_subreddit)
       
        logger.info("Starting the Faktory consumer to listen for jobs...")

        consumer.run()

        continuous_schedule(SUBREDDIT)        # Start consuming jobs

        # Start scheduling crawls continuously
