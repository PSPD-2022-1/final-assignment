#!/bin/bash

fail_if_unset_or_empty() {
	local -r VAR_NAME="${1}"
	local -r VALUE_ERROR_MSG="variable '${VAR_NAME}' is not set or is empty"
	local -r VAR_NOT_FOUND_ERROR_MSG="variable '${VAR_NAME}' was not found"

	echo "${VAR_NAME:?${VAR_NOT_FOUND_ERROR_MSG}}=${!VAR_NAME:?${VALUE_ERROR_MSG}}"
}

HADOOP_USER=''
HADOOP_BIN=''
HDFS_BIN=''
SPARK_BIN=''
KAFKA_TOPICS_BIN=''
KAFKA_CONSUMER_BIN=''

fail_if_unset_or_empty "HADOOP_USER"
fail_if_unset_or_empty "HADOOP_BIN"
fail_if_unset_or_empty "HDFS_BIN"
fail_if_unset_or_empty "SPARK_BIN"
fail_if_unset_or_empty "KAFKA_TOPICS_BIN"
fail_if_unset_or_empty "KAFKA_CONSUMER_BIN"

SPARK_SHUFFLE_SERVICE_PORT="7337"
SPARK_SQL_SHUFFLE_PARTITIONS="1000"

SPARK_KAFKA_STREAM_JAR='twitter-spark-words-kafka-stream-1.0-SNAPSHOT.jar'
SPARK_KAFKA_STREAM_PRODUCER_JAR_CLASS='pspd.spark.WordProducer'
SPARK_KAFKA_STREAM_CONSUMER_JAR_CLASS='pspd.spark.WordConsumer'

HDFS_SPARK_KAFKA_STREAM_DIR="/user/${HADOOP_USER}/spark-kafka-words"
HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR="${HDFS_SPARK_KAFKA_STREAM_DIR}/output"
HDFS_SPARK_KAFKA_STREAM_CHECKPOINT_DIR="${HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR}/checkpoint"

TWITTER_BEARER_TOKEN=''

KAFKA_ADDRESSES="localhost:9092"

INPUT_TOPIC_NAME="tweets"

OUTPUT_WORDS_TOPIC_NAME="tweetWords"

TOPICS=("$INPUT_TOPIC_NAME" "$OUTPUT_WORDS_TOPIC_NAME")
