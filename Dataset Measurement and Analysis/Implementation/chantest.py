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
DATABASE_URL = os.getenv('DATABASE_URL_CHAN')
FAKTORY_SERVER_URL = os.getenv('FAKTORY_SERVER_URL')

# Hate Speech API credentials
HS_API_TOKEN = os.getenv('HS_API_TOKEN_FINAL')

# Register the adapter for psycopg2 to insert dicts into a jsonb column
register_adapter(dict, Json)

def hs_check_comment(comment):
    """Check a comment for hate speech using the API and return the raw score."""
    try:
        logger.info(comment)
        data = {"token": HS_API_TOKEN, "text": comment}
        response = requests.post(os.getenv('HS'), json=data)

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

def is_valid_timestamp(timestamp_string):
    """Check if the timestamp is between November 1 and November 14, 2024."""
    try:
        # Parsing the timestamp_string to a datetime object
        timestamp = datetime.datetime.strptime(timestamp_string, "%m/%d/%y(%a)%H:%M:%S")

        # Defining the range: November 1, 2024 to November 14, 2024
        start_date = datetime.datetime(2024, 11, 1)
        end_date = datetime.datetime(2024, 11, 14, 23, 59, 59)

        # Check if the timestamp falls within the range
        if start_date <= timestamp <= end_date:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error parsing timestamp {timestamp_string}: {e}")
        return False

def process_data_with_hate_speech(batch_size=1000):
    """Fetch data in batches from the database, process hate speech scores, and store results in another table."""
    conn = psycopg2.connect(dsn=DATABASE_URL)
    cur = conn.cursor()

    logger.info("Processing data for hate speech detection in batches...")

    try:
        # Create the table for processed data if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS preprocessed_json_postss (
            id TEXT PRIMARY KEY,
            raw_data JSONB,
            height INTEGER,
            width INTEGER,
            comment TEXT,
            file_extension TEXT,
            md5_hash TEXT,
            timestamp_string TEXT,
            unix_timestamp BIGINT,
            poster_name TEXT,
            poster_time BIGINT,
            thumbnail_height INTEGER,
            thumbnail_width INTEGER,
            file_size INTEGER,
            resto INTEGER,
            closed BOOLEAN,
            images_count INTEGER,
            country_code TEXT,
            replies_count INTEGER,
            archived BOOLEAN,
            filename TEXT,
            bumplimit INTEGER,
            imagelimit INTEGER,
            archived_on BIGINT,
            country_name TEXT,
            semantic_url TEXT,
            sub TEXT,
            m_img INTEGER,
            hs_raw_score FLOAT
        )
        """
        cur.execute(create_table_query)
        conn.commit()

        # Get the total count of rows to process
        cur.execute("SELECT COUNT(*) FROM posts")
        total_rows = cur.fetchone()[0]
        logger.info(f"Total rows to process: {total_rows}")

        offset = 0

        while offset < total_rows:
            logger.info(f"Processing batch starting at offset {offset}...")

            # Fetch a batch of data
            cur.execute(
                """
                SELECT id, data
                FROM posts
                ORDER BY id
                LIMIT %s OFFSET %s
                """,
                (batch_size, offset)
            )
            rows = cur.fetchall()

            if not rows:
                logger.info("No more rows to process.")
                break

            for row in rows:
                try:
                    id, raw_data = row

                    # Ensure `raw_data` is a dict
                    if isinstance(raw_data, str):
                        data = json.loads(raw_data)  # Parse JSON string into a dictionary
                    elif isinstance(raw_data, dict):
                        data = raw_data  # Already a dictionary
                    else:
                        raise ValueError(f"Unexpected type for raw_data: {type(raw_data)}")

                    # Extract comment and timestamp_string
                    body = data.get('com', '')
                    timestamp_string = data.get('now', '')

                    # Check if the timestamp is within the valid range
                    if not is_valid_timestamp(timestamp_string):
                        logger.info(f"Skipping id={id} as timestamp is not within the valid range.")
                        continue

                    # Perform hate speech detection
                    raw_score = hs_check_comment(body)

                    logger.info(f"Raw Score= {raw_score}")

                    # Insert processed data into the new table
                    insert_query = """
                    INSERT INTO preprocessed_json_postss (
                        id, raw_data, height, width, comment, 
                        file_extension, md5_hash, timestamp_string, 
                        unix_timestamp, poster_name, poster_time, 
                        thumbnail_height, thumbnail_width, file_size, 
                        resto, closed, images_count, country_code, 
                        replies_count, archived, filename, bumplimit, 
                        imagelimit, archived_on, country_name, 
                        semantic_url, sub, m_img, hs_raw_score
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """
                    cur.execute(
                        insert_query,
                        (
                            id,  # id
                            Json(data),  # raw_data
                            data.get('h'),  # height
                            data.get('w'),  # width
                            data.get('com', ''),  # comment
                            data.get('ext'),  # file_extension
                            data.get('md5'),  # md5_hash
                            data.get('now'),  # timestamp_string
                            data.get('tim'),  # unix_timestamp
                            data.get('name'),  # poster_name
                            data.get('time'),  # poster_time
                            data.get('tn_h'),  # thumbnail_height
                            data.get('tn_w'),  # thumbnail_width
                            data.get('fsize'),  # file_size
                            data.get('resto'),  # resto
                            data.get('closed') == 1,  # closed (convert to boolean)
                            data.get('images'),  # images_count
                            data.get('country'),  # country_code
                            data.get('replies'),  # replies_count
                            data.get('archived') == 1,  # archived (convert to boolean)
                            data.get('filename'),  # filename
                            data.get('bumplimit'),  # bumplimit
                            data.get('imagelimit'),  # imagelimit
                            data.get('archived_on'),  # archived_on
                            data.get('country_name'),  # country_name
                            data.get('semantic_url'),  # semantic_url
                            data.get('sub', ''),  # sub
                            data.get('m_img'),  # m_img
                            raw_score  # hs_raw_score
                        ),
                    )
                    conn.commit()
                    logger.info(f"Inserted row for id={id}")

                except Exception as e:
                    logger.error(f"Failed to process record for id={id}: {e}")

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
    client = Client(url=FAKTORY_SERVER_URL)
    job = Job(
        'HateSpeechProcessingJob',
        args={'batch_size': 1000},
        queue='default'
    )
    client.push(job)
    logger.info("Scheduled job to process hate speech.")

if __name__ == "__main__":
    try:
        process_data_with_hate_speech()  # Main processing function
    except Exception as e:
        logger.error(f"Error during execution: {e}")