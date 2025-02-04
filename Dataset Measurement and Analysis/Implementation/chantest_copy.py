from pyfaktory import Client, Consumer, Job, Producer
import time
import datetime
import logging
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import os
from dotenv import load_dotenv
import requests
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
DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/chan_crawler"
FAKTORY_SERVER_URL = 'tcp://:password@localhost:7419'

# Register the adapter for psycopg2 to insert dicts into a jsonb column
register_adapter(dict, Json)

HS_API_TOKEN = "b21a34635d2a05f89d36babc0c9734cc"

def hs_check_comment(comment):
    """Check a comment for hate speech using the API and return the raw score."""
    try:
        data = {"token": HS_API_TOKEN, "text": comment}
        response = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data)
        if response.status_code == 200:
            result = response.json()
            raw_class = result.get("class", "")
            raw_score = result.get("confidence", 0.0)
            return raw_class
        else:
            logger.error(f"Hate speech API error: {response.status_code}, {response.text}")
            return ""
    except Exception as e:
        logger.error(f"Error in hate speech check: {e}")
        return ""

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
        CREATE TABLE IF NOT EXISTS finalfast_preprocessed_json_postss (
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
            hs_raw_score TEXT
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
                    INSERT INTO finalfast_preprocessed_json_postss (
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
    logger.info("Scheduling hate speech processing job...")
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3)
        run_at_iso = run_at.isoformat()
        job = Job(jobtype="process-hate-speech", args=(), queue="hate-speech", at=run_at_iso)
        producer.push(job)

def continuous_schedule():
    """Continuously schedule the hate speech processing job."""
    while True:
        schedule_hate_speech_processing()
        time.sleep(1)

if __name__ == "__main__":
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["hate-speech"], concurrency=10)
        consumer.register("process-hate-speech", process_data_with_hate_speech)
        logger.info("Starting Faktory consumer...")
        # continuous_schedule()
        consumer.run()
        continuous_schedule()