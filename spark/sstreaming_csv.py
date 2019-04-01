from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,window
from pyspark.sql.functions import split,from_json,col
from pyspark.sql.types import StructType,StringType,IntegerType,TimestampType
import time

def main():
    # define spark session
    print('define session')
    spark = (SparkSession \
            .builder \
            .master('local[*]')\
            .appName("Number of comments") \
            .getOrCreate()
            )
    spark.sparkContext.setLogLevel("ERROR")
    # source
    print('source')
    schema = StructType()\
        .add('post',StringType())\
        .add('subreddit',StringType())\
        .add('timestamp',TimestampType())
    #jsontimestampformat = "yyyy-MM-dd HH:mm:ss"
    #jsonOptions = { "timestampFormat": jsontimestampformat }
    # lines = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "reddit-stream-topic") \
    #     .option("startingOffsets", "latest")\
    #     .load()
    inputPath = '/Users/akhileshsk/github/reddit_analyzer/spark/data/'
    lines = (
          spark
            .readStream
            .schema(schema)
            .option("sep", ",")              # Set the schema of the JSON data
            .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
            .csv(inputPath)
        )
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

    #lines = lines.filter(lines.post.like('%python%'))
    rowCounts = lines.groupBy('post',window('timestamp','10 seconds','2 second')).count().orderBy('count',ascending = 0)

    # sink
    print('sink')
    #spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

    query =(rowCounts \
        .writeStream \
        .queryName("aggregates")\
        .outputMode("complete") \
        .format("console") \
        .start()
        )
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
