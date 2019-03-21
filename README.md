# reddit_analyzer
Analysis of reddit for ads

Project:
Analytics for advertisements on Reddit comment and post dataset.



Motivation and BusinessValue:
-----------------------------

Reddit has a huge user base,1.65 billion users accessed the site in the last 1 year
and advertisers need to know where to place ads, 

Two main questions

    1. which subreddit? 
    2. What are users taking about?
    
Metrics: 
----------

1. hottest subreddit( according to the number of comments and score, assuming 500:1 ratio for users to comments) at a given time, 
2. topics talked about in the subreddit vs time, 
3. Number of users or Number of Comments vs hour of the day histogram.
4. Stretch goal 1:Best posts to advertise on, depending on theme.( gender, topic,keyowords)
5. Stretch goal 2: would be track simulated engagement, time spent on a post by users.

Data Source:
--------------
500 GB+, comments,  posts,  subreddits
1. https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit_comments
2. https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit
3. https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit_posts

Tack stack:
-------------

1. Storage on S3
2. Kafka
3. Spark ( LDA library)
4. Store results to Cassandra
5. Store latest results from Cassandra to redis(C++) for fast access
6. Dashboard

Engineering Challenge:
----------------------

Realtime processing of all subreddits according to timestamp (speed up) in 50 ms window
Scaling the system with increase in the traffic

Next steps:
--------------

1. Learn Kafka
2. Switch to java?


