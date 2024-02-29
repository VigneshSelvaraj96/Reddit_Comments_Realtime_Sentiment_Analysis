import praw
import time
from kafka import KafkaProducer


def auth_reddit():
    # Create a connection to the Reddit API
    reddit = praw.Reddit(
    client_id="-0zelvLs3NH6lZEu5I5uOQ",
    client_secret="JSsDEgmScZkIb1UDhqmVLxL0t2rofA",
    user_agent="android:com.example.myredditapp:v1.2.3 (by u/vig1818)")
    return reddit

if __name__ == "__main__":
    # Create a connection to the Reddit API
    reddit = auth_reddit()
    # Create a connection to the Kafka API
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic_kafka = 'mytopic'

    #loop through 10 submissions from the cars subreddit and send the comments to the Kafka API
    subreddit = reddit.subreddit("cars")
    for submission in subreddit.hot(limit=10):
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            #periodically print comments to the console and send them to the Kafka API
            time.sleep(5)
            #print(comment.body)
            #print('-------------------\n')
            producer.send(topic_kafka,key = subreddit.name.encode('utf-8'), value=comment.body.encode('utf-8'))
