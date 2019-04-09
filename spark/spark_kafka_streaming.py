from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,window,countDistinct
from pyspark.sql.functions import split,from_json,col,to_json,struct
from pyspark.sql.types import StructType,StringType,IntegerType,TimestampType
import time

def main():
    # define spark session
    print('define session')
    spark = (SparkSession \
            .builder \
            .appName("Number of comments") \
            .getOrCreate()
            )
    #.master('local[2]')\
    spark.sparkContext.setLogLevel("ERROR")
    # source
    print('source')
    schema = StructType()\
        .add('post',StringType())\
        .add('subreddit',StringType())\
        .add('timestamp',TimestampType())\
        .add('body',StringType())\
        .add('author',StringType())
    #jsontimestampformat = "yyyy-MM-dd HH:mm:ss"
    #jsonOptions = { "timestampFormat": jsontimestampformat }
    # lines = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "reddit-stream-topic") \
    #     .option("startingOffsets", "latest")\
    #     .load()
    #inputPath = '/Users/akhileshsk/github/reddit_analyzer/spark/data/'
    lines = (
          spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", "10.0.0.5:9092")\
            .option("subscribe", "reddit-stream-topic")\
            .option("startingOffsets","latest")\
            .load()
        )
    #
    lines  = lines.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"),schema=schema).alias('data')).select('data.*')
    #lines = lines.selectExpr("CAST(value AS STRING)")
    lines.printSchema()

    #lines = lines.select(from_json(col("value").cast("string"),schema).alias('data'))
    #lines.printSchema()

    print('source selected')
    # query
    print('query')
    #temp = lines.createOrReplaceTempView("updates")
    #rowCounts = spark.sql("select count(*) as rows from updates")

    #rowCounts.printSchema()
    # selsubreddits = lines.select('subreddit','post','timestamp')#.alias('subreddit')
    # selsubreddits.printSchema()
    # #rowCounts = selsubreddits.select('subreddit')#.select('subreddit')#.groupBy("subreddit").count()#.orderby(desc)
    # rowCounts = (selsubreddits\
    #     .groupBy(selsubreddits.post,window(selsubreddits.timestamp,'10 seconds','5 seconds')).count()\
    #     )
        #.groupBy(col('post')).count())
        #.groupBy(window(selsubreddits.created_utc, "30 seconds", "5 seconds"),selsubreddits.post).count()
        #.select(window(col('created_utc'), "1 minute", "10 seconds"),col('post'))

        #.groupBy(col('post')).count()
    #rowCounts.take(5)
    #lines.createOrReplaceTempView("table")

    #lines = lines.filter(lines.body.like('%sports%'))
    #########
    # 1. unique users
    #unique_users =
    # unique number of comments of a given post in a given timewindow
    # 2. best posts by comments
    #lines.createOrReplaceTempView("reddit")
    #rowCounts = spark.sql('SELECT COUNT( DISTINCT author) FROM reddit')
    #unique_users = lines.agg(countDistinct(lines.post,lines.author))
    rowCounts = lines.groupBy('post',window('timestamp','300 seconds','10 second')).count().orderBy('count',ascending = 0)
    # join dataframes
    # 3. filter posts by keywords in post body and create seperate column for each
    # schema : unique_users_keyword, num_comments_keyword, window,

    # sink
    print('sink')
    #spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

    # query =(rowCounts \
    #     .writeStream \
    #     .queryName("aggregates")\
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()
    #     )
    query =(rowCounts \
        .select(to_json(struct("post", "window","count")).cast("string").alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.0.0.5:9092") \
        .option("topic", "reddit-spark-topic") \
        .option("checkpointLocation", "/home/ubuntu/spark/checkpoint/") \
        .outputMode("complete") \
        .start()
        )
        #.option("failOnDataLoss","false")\

        #
        # .option("checkpointLocation",'checkpoint/')\
        # .option('path','rowcount/')\
    # print('sql')
    # for i in range(100):
    #     spark.sql("select * from aggregates").show()
    #     time.sleep(5)
    #print(rowCounts.isStreaming)

    query.awaitTermination()
    return
if __name__ == '__main__':
    main()
