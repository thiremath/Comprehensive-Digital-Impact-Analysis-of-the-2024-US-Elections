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
# from datetime import datetime

# Setup logging
logger = logging.getLogger("HateSpeechProcessor")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# Load environment variables
load_dotenv()

# Environment variables
DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_crawler"
FAKTORY_SERVER_URL = 'tcp://:password@localhost:7419'

# Hate Speech API credentials
HS_API_TOKEN = "f7cad5eb69c78c601b52ce77fd5ea1d3"

# Register the adapter for psycopg2 to insert dicts into a jsonb column
register_adapter(dict, Json)

def hs_check_comment(comment):
    """Check a comment for hate speech using the API and return the raw score."""
    try:
        data = {"token": HS_API_TOKEN, "text": comment}
        response = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data)

        if response.status_code == 200:
            result = response.json()
            raw_score = result.get("confidence", 0.0)  # Get raw confidence score
            return raw_score
        else:
            logger.error(f"Hate speech API error: {response.status_code}, {response.text}")
            return 0.0
    except Exception as e:
        logger.error(f"Error in hate speech check: {e}")
        return 0.0

def process_data_with_hate_speech(batch_size=1000):
    """Fetch data in batches from the database, process hate speech scores, and store results in another table."""
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    logger.info("Processing data for hate speech detection in batches...")

    try:
        # Create the table for processed data if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ALL_DATA_preprocessed_reddit_posts (
            id TEXT PRIMARY KEY,
            ups TEXT,
            body TEXT,
            name TEXT,
            downs TEXT,
            likes TEXT,
            author TEXT,
            link_id TEXT,
            over_18 TEXT,
            replies TEXT,
            banned_by TEXT,
            parent_id TEXT,
            permalink TEXT,
            subreddit TEXT,
            created_utc TEXT,
            link_author TEXT,
            comment_type TEXT,
            num_comments TEXT,
            subreddit_id TEXT,
            banned_at_utc TEXT,
            link_permalink TEXT,
            subreddit_type TEXT,
            controversiality TEXT,
            author_is_blocked TEXT,
            unrepliable_reason TEXT,
            subreddit_name_prefixed TEXT,
            hs_raw_score FLOAT
        );
        """

        cur.execute(create_table_query)
        conn.commit()

        # Get the total count of rows to process
        cur.execute("SELECT COUNT(*) FROM reddit_posts")
        total_rows = cur.fetchone()[0]
        logger.info(f"Total rows to process: {total_rows}")

        offset = 0

        while offset < total_rows:
            logger.info(f"Processing batch starting at offset {offset}...")

            # Fetch a batch of data
            cur.execute(
                """
                SELECT post_id, title, subreddit, author, score, created_utc, data
                FROM reddit_posts
                ORDER BY created_utc
                LIMIT %s OFFSET %s
                """,
                (batch_size, offset)
            )
            rows = cur.fetchall()

            if not rows:
                logger.info("No more rows to process.")
                break

            for row in rows:
                post_id, title, subreddit, author, score, created_utc, data = row

                id = data.get("id", "")
                ups = data.get("ups", "")
                body = data.get("body", "")
                name = data.get("name", "")
                downs = data.get("downs", "")
                likes = data.get("likes", "")
                #     datetime.utcfromtimestamp(data.get("created", None)) if data.get("created", None) else None
                # )
                link_id = data.get("link_id", "")
                over_18 = data.get("over_18", "")
                replies = data.get("replies", "")
                banned_by = data.get("banned_by", "")
                parent_id = data.get("parent_id", "")
                permalink = data.get("permalink", "")
                link_author = data.get("link_author", "")
                comment_type = data.get("comment_type", "")
                num_comments = data.get("num_comments", "")
                subreddit_id = data.get("subreddit_id", "")
                banned_at_utc = data.get("banned_at_utc", "")
                link_permalink = data.get("link_permalink", "")
                subreddit_type = data.get("subreddit_type", "")
                controversiality = data.get("controversiality", "")
                author_is_blocked = data.get("author_is_blocked", "")
                unrepliable_reason = data.get("unrepliable_reason", "")
                subreddit_name_prefixed = data.get("subreddit_name_prefixed", "")





                # Perform hate speech detection
                raw_score = hs_check_comment(body)

                logger.info(f"Raw Score= {raw_score}")

                # Insert processed data into the new table
                try:
                    insert_query = """
                        INSERT INTO ALL_DATA_preprocessed_reddit_posts (
                            id, ups, body, name, downs, likes, author,link_id, over_18, replies,banned_by,
                            parent_id, permalink, subreddit, created_utc, link_author, comment_type, num_comments, subreddit_id,
                            banned_at_utc, link_permalink, subreddit_type,
                            controversiality,author_is_blocked,
                            unrepliable_reason,
                            subreddit_name_prefixed, hs_raw_score
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (id) DO NOTHING;
                    """

                    cur.execute(
                        insert_query,
                        (
                            id, ups, body, name, downs, likes, author,  link_id, over_18, replies,banned_by,
                            parent_id, permalink, subreddit, created_utc, link_author, comment_type, num_comments, 
                            subreddit_id, banned_at_utc, link_permalink, subreddit_type,
                            controversiality, author_is_blocked,
                            unrepliable_reason, 
                            subreddit_name_prefixed, raw_score
                        )
                    )

                    conn.commit()  # Explicitly commit each row
                    logger.info(f"Inserted row for post_id={post_id}")
                except Exception as e:
                    logger.error(f"Failed to insert row for post_id={post_id}: {e}")

                logger.info(f"Inserted a row *******")

            conn.commit()
            logger.info(f"Batch starting at offset {offset} processed successfully.")
            offset += batch_size  # Increment offset for the next batch

        logger.info("All batches processed successfully.")
    except Exception as e:
        logger.error(f"Failed to process data: {e}")
    finally:
        cur.close()
        conn.close()

def schedule_hate_speech_processing():
    """Schedule the hate speech processing job."""
    logger.info("Scheduling hate speech processing job...")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)

        # Use timezone-aware datetime for UTC
        run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)  # 10-second delay
        run_at_iso = run_at.isoformat()

        job = Job(jobtype="process-hate-speech", args=(), queue="hate-speech", at=str(run_at_iso))
        producer.push(job)

def continuous_schedule():
    """Continuously schedule the hate speech processing job."""
    while True:
        schedule_hate_speech_processing()
        time.sleep(5)  # Adjust the interval as needed

if __name__ == "__main__":
    # Set up the Faktory consumer to handle jobs
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["hate-speech"], concurrency=1)

        # Register job types
        consumer.register("process-hate-speech", process_data_with_hate_speech)

        logger.info("Starting the Faktory consumer to listen for hate speech jobs...")
        # continuous_schedule()
        consumer.run()

        continuous_schedule()  # Start scheduling jobs continuously