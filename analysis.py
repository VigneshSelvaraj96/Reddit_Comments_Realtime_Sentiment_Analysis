from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from textblob import TextBlob
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import re


mongoURL = "mongodb+srv://vxs377:1234@vig.h1kud88.mongodb.net/?retryWrites=true&w=majority&appName=Vig"
#connect to mongoDB
# Create a new client and connect to the server
client = MongoClient(mongoURL, server_api=ServerApi('1'))
db = client.vig
collection = db.vig
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

def cleancomment_func(comment: str) -> str:
    comment = re.sub(r'http\S+', '', str(comment))
    comment = re.sub(r'bit.ly/\S+', '', str(comment))
    comment = comment.strip('[link]')

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    comment = re.sub('[' + my_punctuation + ']+', ' ', str(comment))

    # remove number
    comment = re.sub('([0-9]+)', '', str(comment))

    # remove hashtag
    comment = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(comment))

    return comment


# Create a function to get the polarity
def getPolarity(comment: str) -> float:
    return TextBlob(comment).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'
    
def write_to_mongo(df, epoch_id):
    #convert the spark dataframe to pandas dataframe
    df = df.toPandas()
    #interate through the pandas dataframe and store each row in the mongoDB cluster
    for index, row in df.iterrows():
        data = {
            "submission_name": row['submission_name'],
            "comment_preprocessing": row['comment_preprocessing'],
            "comment_postprocessing": row['comment_postprocessing'],
            "polarity": row['polarity'],
            "sentiment": row['sentiment']
        }
        collection.insert_one(data)
    print("Data written to MongoDB")



if __name__ == '__main__':
    # Create a SparkSession
    spark = SparkSession.builder\
            .appName("RedditSentimentAnalysis")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")\
            .getOrCreate()

    df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "mytopic") \
            .load()


    #print schema
    #df.printSchema()
    #print("Schema printed")
    #typecast the key column to string
    df_kafka = df_kafka.withColumn("key", col("key").cast("string"))
    #typecast the value column to string
    df_kafka = df_kafka.withColumn("value", col("value").cast("string"))
    #rename the value column to comment_preprocessing
    df_kafka = df_kafka.withColumnRenamed("value", "comment_preprocessing")
    #rename the key column to subreddit_name
    df_kafka = df_kafka.withColumnRenamed("key", "submission_name")
    #create udf for cleaning the comment
    #apply the udf to the comment_preprocessing column and store in a new colunm 'comment_postprocessing'
    df_kafka = df_kafka.withColumn("comment_postprocessing", F.udf(cleancomment_func, StringType())(col("comment_preprocessing")))
    #store the polarity of the cleaned comment in a new column 'polarity'
    df_kafka = df_kafka.withColumn("polarity", F.udf(getPolarity, FloatType())(col("comment_postprocessing")))
    #store the sentiment of the cleaned comment in a new column 'sentiment'
    df_kafka = df_kafka.withColumn("sentiment", F.udf(getSentiment, StringType())(col("polarity")))
    #send the new, processed row to the mongoDB cluster for storage
    df_kafka.writeStream.foreachBatch(write_to_mongo).start().awaitTermination(100)
    #df_kafka.printSchema()

    