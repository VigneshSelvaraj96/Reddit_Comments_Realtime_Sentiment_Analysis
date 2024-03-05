Technology stack:
* Apache kafka: Version kafka_2.13-3.6.1
* Python: Version 3.11.8
* Spark: spark-connector @ org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4
* MongoDB: Cloud NoSQL Storage
* Tableau: Desktop with MongoDB Atlas Connector + latest taco file




This is a personal project where I stream data from reddit API's on a particular subreddit (Example 'Bikes') and pull comments through a python script called Producer.py. 
I then set up a local apache kafka cluster on my machine where I send the post submission's title as a key, the comments themselves as values to a topic called mytopic to store the streaming
comments in a Kafka Producer.

Just a little bit of a primer on kafka, 
* Kafka is a distributed event streaming platform that lets you read, write, store, and process events (also called records or messages in the documentation) across many machines.

* Example events are payment transactions, geolocation updates from mobile phones, shipping orders, sensor measurements from IoT devices or medical equipment, and much more. These events are   
  organized and stored in topics. Very simplified, a topic is similar to a folder in a filesystem, and the events are the files in that folder.

Analysis.py uses a local spark cluster and spark session to process the records stored in the producer cluster in apache kafka (acts like a kafka consumer) to
1. Clean the text data in the Kafka Cluster
2. Parse the data into a defined schema 
3. Do sentiment analysis on the comments
4. Attach a new column for the polarity and sentiment classification to the schema
5. Store the dataframe stream to a mongodb cluster that triggers on each batch. 

To do some data visualization, I use tableau + MongoDB BI connector for tableau to stream the NoSQL data as records and aggregate the count of the different polarities 
grouped by submission names to show the overall subreddit's polarity.
