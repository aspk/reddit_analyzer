# reddit_analyzer
Analysis of reddit for ads

Project:
Analytics for advertisements on Reddit comment and post dataset.



Motivation and BusinessValue:
-----------------------------


Reddit has a huge user base,1.65 billion users accessed the site in the last 1 year
Advertisers need to know where to place ads, 
which subreddit? 
What are users taking about?
Metrics: 
hottest subreddit( according to the number of comments and score, assuming 500:1 ratio for users to comments) at a given time, 
topics talked about in the subreddit vs time, 
Number of users or Number of Comments vs hour of the day histogram.
Stretch goal 1:Best posts to advertise on, depending on theme.( gender, topic,keyowords)
Stretch goal 2: would be track simulated engagement, time spent on a post by users.
Data Source:
--------------

https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit_comments
https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit
Tack stack:
-------------

Storage on S3
Kafka
Spark ( LDA library)
Store results to Cassandra
Store latest results from Cassandra to redis(C++) for fast access
Dashboard




Engineering Challenge:
----------------------

Realtime processing of all subreddits according to timestamp (speed up) in 50 ms window
Scaling the system with increase in the traffic

Next steps:
--------------

Learn Kafka
Switch to java?


