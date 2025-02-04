# Social Media Data Science Pipelines Project 2

### Authors
- **Devang Jagdale** (djagdale@binghamton.edu)
- **Tejas Hiremath** (thiremath@binghamton.edu)
- **Chaitanya Jha** (cjha@binghamton.edu)
- **Institution**: Binghamton University, Binghamton, New York, USA


This project focuses on processing and analyzing social media data, specifically from the **r/politics** subreddit and the **4chan /pol/** board, to provide insights into the toxicity of content and submission trends. The project implements real-time toxicity measurement using the **ModerateHatespeech API** and includes visualizations for sentiment analysis and submission trends.

## Table of Contents

- [Introduction](#introduction)
- [Dataset Description](#dataset-description)
- [Methodology](#methodology)
- [Results and Discussion](#results-and-discussion)
- [Technologies](#technologies)

## Introduction

The goal of this project is to transform raw social media data into actionable insights through measurement and analysis. By analyzing sentiment trends and quantifying toxicity, this work provides valuable insights into the emotional engagement and reactions of online communities, especially related to political discussions during the 2024 U.S. elections.

## Dataset Description

The datasets used for this project include submissions and comments from the following sources:

- **r/politics subreddit** (Reddit)
- **/pol/ board** (4chan)

The data was collected between **November 1, 2024, and November 14, 2024**, and includes metrics such as the number of daily submissions, hourly comments, and sentiment analysis for various emotional categories (angry, happy, hope, sad).

## Methodology

1. **Real-time Toxicity Scoring**: 
   - Integrated **ModerateHatespeech API** into the data pipeline to measure toxicity in real time.
   
2. **Data Transformation**: 
   - Extracted submission and comment trends from both datasets.
   
3. **Sentiment Analysis**: 
   - Categorized comments into four distinct emotional categories: **angry**, **happy**, **hope**, and **sad**, excluding neutral comments.

4. **Visualization**: 
   - Utilized Python tools (PgAdmin, Jupyter Notebook, Google Colab) for data processing and visualization of key metrics.

## Results and Discussion

### Sentiment Analysis (Reddit and 4chan)

- **Reddit**: The sentiment analysis revealed significant spikes in **hope** and **anger** between November 5th and 7th, 2024, reflecting strong emotional responses to major political events.
  
- **4chan**: Similarly, **hope** and **anger** were the dominant sentiments, with noticeable spikes around the same time frame.

### Submission and Comment Activity

- **Reddit**: Daily and hourly comment trends show a consistent periodic pattern, with a notable spike on **November 6th**, correlating with major political events.
  
- **4chan**: The post count on 4chan was consistently higher than Reddit, likely due to its unmoderated environment, which fosters greater user engagement.

### Election Narratives

- **Election Narratives**: A bar graph analysis showed a strong focus on the **Republican Party** in election-related discussions, with the "red wave" narrative being far more prominent than the "blue wave" narrative.

### Cumulative Distribution of Comment Lengths

- Most comments were relatively short, with a significant concentration of comments between 0 and 200 characters.

## Technologies

- **Programming Languages**: Python
- **Data Storage**: PostgreSQL
- **Visualization**: Matplotlib, Jupyter Notebook
- **API**: ModerateHatespeech
- **Web Scraping**: Reddit API, 4chan Data Collection Scripts
- **Environment**: Google Colab, PgAdmin


## Instructions to run the project
There are two files which need to be run
commands-
python3 test.py
python3 chantest_copy.py


Dataset is too large. Contact the authors to obtain it.


Upload the ipynb files- Analysis1, Analysis2, Analysis3 from google drive.