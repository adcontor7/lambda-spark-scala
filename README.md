# lambda-spark-scala
Lambda Arquitecture example with Apache Spark in Scala.

Processed data is stored in a Redis database with these Key prefixes:
 - RTView1::<key> -> First Real Time view
 - RTView2::<key> -> Second Real Time view
 - BView::<key> -> Batch View

# Build lambda-arquitecture Spark
1. $ docker build --rm=true -t adcontor7/spark-scala-template spark-scala-template/ 
2. $ docker build --rm=true -t adcontor7/lambda-spark lambda-spark/
# Create Spark Context to test the code
1. $ docker run -it --rm adcontor7/lambda-spark sbt console 

# Up infraestructure
1. $ docker-compose up -d
Check Haddop http://localhost:50070/dfshealth.html
2. $ docker-compose exec namenode hdfs dfs -mkdir /new
3. $ docker-compose exec namenode hdfs dfs -mkdir /raw
4. $ docker-compose exec namenode hdfs dfs -ls /

# Kafka
1. kafkacat -L -b localhost:9092
2. $ docker-compose exec kafka sh /usr/bin/kafka-console-producer --broker-list localhost:9092 --property 'parse.key=true'  --property 'key.separator=:'  --topic wordcount
> a:10
> b:20
> a:5

#Redis
dc exec redis redis-cli get <KEY>





#Clear DataSets
1. dc exec namenode hdfs dfs -rm -r /new/*
2. dc exec namenode hdfs dfs -rm -r /raw/*
