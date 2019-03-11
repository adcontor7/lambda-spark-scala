FROM adcontor7/spark-scala-template:1.1.0-hadoop2.7

MAINTAINER Adrián Contreras <adcontor7@gmail.com>

ENV SPARK_APPLICATION_MAIN_CLASS adcontor7.lambda.spark.Application
ENV SPARK_APPLICATION_ARGS "kafka:9092 wordcount redis:6379 hdfs://hadoop-namenode/"
