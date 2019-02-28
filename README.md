# lambda-spark-scala
Lambda Arquitecture example with Apache Spark in Scala


1. $ docker-compose up -d
Check Haddop http://localhost:50070/dfshealth.html
2. $ docker-compose exec namenode hdfs dfs -mkdir /new
3. $ docker-compose exec namenode hdfs dfs -mkdir /master
4. $ docker-compose exec namenode hdfs dfs -ls /

# Kafka
1. kafkacat -L -b localhost:9092
2. $ docker-compose exec kafka sh /usr/bin/kafka-console-producer --broker-list localhost:9092 --property 'parse.key=true'  --property 'key.separator=:'  --topic example

#Redis
dc exec redis redis-cli get <KEY>
