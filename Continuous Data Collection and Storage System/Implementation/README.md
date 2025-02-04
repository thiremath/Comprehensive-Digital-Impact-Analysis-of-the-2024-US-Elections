# Social Media Data Science Pipelines Project 1: 4chan and Reddit Data Collection System

### Authors
- **Devang Jagdale** (djagdale@binghamton.edu)
- **Tejas Hiremath** (thiremath@binghamton.edu)
- **Chaitanya Jha** (cjha@binghamton.edu)
- **Institution**: Binghamton University, Binghamton, New York, USA

## Project Overview

This project is part of a series of data science pipelines aimed at collecting and analyzing social media data. The primary objective of **Project 1** is to create a continuous data collection system from two social media platforms: **Reddit** and **4chan**. The collected data will support future data science projects, such as **sentiment analysis**, **hate speech detection**, and the broader study of online discourse.

Data is dynamically fetched from selected **subreddits** and **4chan boards** and is processed and stored in a **TimescaleDB-powered PostgreSQL database** for efficient time-series analysis.

## Data Sources

### Reddit API
The project utilizes the Reddit API for structured access to posts and comments from specific subreddits. The data collected focuses on:
- **Posts** and **comments** from user-generated content within targeted subreddits.
  
Key endpoints used:
- **Search API**: `https://oauth.reddit.com/r/{subreddit}/search` (Fetches posts sorted by newest).
- **Post Details API**: `https://oauth.reddit.com/r/{subreddit}/comments/{post_id}` (Retrieves full post content by post ID).

### 4chan API (Scraping)
Since 4chan does not provide an official API, the project scrapes data from specific boards (e.g., `/pol/`):
- **Catalog API**: `http://a.4cdn.org/pol/catalog.json` (Retrieves metadata from active threads on /pol/).
- **Thread API**: `http://a.4cdn.org/pol/thread/{thread_number}.json` (Retrieves all posts from a specific thread).

## Data Collection Workflow

### Crawler Classes
Two Python clients manage the data collection process:
- **RedditClient**: Handles API interactions for fetching posts and comments from subreddits.
- **ChanClient**: Scrapes 4chan boards to retrieve threads and posts from the `/pol/` board.

### Process Flow
1. **Fetch Latest Posts/Threads**: Queries APIs to collect new posts or threads.
2. **Detect Inactive Threads**: Monitors threads and stops fetching data when they are no longer active.
3. **Data Storage**: Preprocesses and stores the collected data in a **TimescaleDB**-powered PostgreSQL database.

## System Architecture

The architecture of the system is designed for scalability and efficiency, consisting of the following components:

- **Scheduler**: Manages the timing of data collection at regular intervals.
- **Crawler**: Collects data from Reddit and 4chan via API requests and scraping.
- **Database**: Stores processed data in a PostgreSQL database with TimescaleDB for time-series analysis.
- **Faktory Job Queue**: Manages background tasks like data fetching and storing, distributing workloads across multiple workers for scalability.

## Implementation Details

### Crawler Scripts
- **reddit.py**: Collects data from Reddit using the API.
- **chan_crawler.py**: Scrapes 4chan boards and collects thread data.
- **cold_start_board.py** and **reddit_coldStart.py**: Initialize data collection from 4chan and Reddit to populate the database with initial datasets.

### Database Management
- **PostgreSQL with TimescaleDB**: Chosen for time-sensitive data storage, enabling efficient time-series queries.
- **Migration Scripts**: Managed with **sqlx** for version control of database schema changes. The database is deployed in a **Docker container** for ease of use.

### Faktory Job Queue
The system leverages **Faktory** to handle background tasks like data collection and storage, ensuring smooth scaling as the data volume increases.

## Challenges Faced

1. **API Rate Limiting**: Reddit enforces rate limits, which required implementing efficient backoff strategies.
2. **HTML Scraping for 4chan**: Unlike Reddit, 4chan lacks a comprehensive API, so data collection relies on HTML scraping.
3. **Storage Constraints**: The system collects large amounts of data, requiring careful consideration of storage resources.

## Preliminary Data Collection Results

Initial data collection from Reddit and 4chan yielded the following results:

- **Reddit**: 25,970 rows collected at 10:54 AM, increasing to 40,756 rows by 4:05 PM.
- **4chan**: 170,209 rows at 10:54 AM, increasing to 195,310 rows by 4:05 PM.

### Data Collection Rates
- **Reddit**: Approximately 5,319 rows/hour (1.48 rows/second).
- **4chan**: Approximately 8,530 rows/hour (2.37 rows/second).

### Projections
- **Per Day**: Reddit: 127,656 rows | 4chan: 204,720 rows
- **Per Week**: Reddit: 893,592 rows | 4chan: 1,432,080 rows
- **Per Month**: Reddit: 3,829,680 rows | 4chan: 6,141,600 rows

## Future Work

The following improvements and future tasks are planned:
1. **Crawler Optimization**: Reducing redundant API calls to improve efficiency.
2. **Storage Scaling**: Requesting additional storage capacity for larger datasets.
3. **Data Preprocessing**: Filtering and cleaning the data to remove irrelevant content.
4. **Data Analysis**: Conducting analyses such as sentiment analysis, hate speech detection, and time-series analysis.

## Conclusion

The 4chan and Reddit Data Collection System is operational and capable of collecting significant volumes of data from both platforms. While challenges like API rate limiting and storage constraints remain, the system has shown promising preliminary results. Future efforts will focus on optimizing data collection processes and performing advanced data analysis.