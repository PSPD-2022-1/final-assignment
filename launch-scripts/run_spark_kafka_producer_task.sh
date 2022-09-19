#!/bin/bash
set -e

source 'env-vars.sh'

fail_if_unset_or_empty "SPARK_BIN"
fail_if_unset_or_empty "SPARK_KAFKA_STREAM_CONSUMER_JAR_CLASS"
fail_if_unset_or_empty "SPARK_SHUFFLE_SERVICE_PORT"
fail_if_unset_or_empty "SPARK_SQL_SHUFFLE_PARTITIONS"
fail_if_unset_or_empty "SPARK_KAFKA_STREAM_JAR"
fail_if_unset_or_empty "KAFKA_ADDRESSES"
fail_if_unset_or_empty "HDFS_SPARK_KAFKA_STREAM_CHECKPOINT_DIR"
fail_if_unset_or_empty "TWITTER_BEARER_TOKEN" >/dev/null

"${SPARK_BIN}" --class "${SPARK_KAFKA_STREAM_PRODUCER_JAR_CLASS}" \
--master yarn \
--deploy-mode cluster \
--driver-memory 5G \
--executor-memory 9G \
--driver-cores 1 \
--executor-cores 4 \
--num-executors 1 \
--conf 'spark.shuffle.service.enabled=true' \
--conf "spark.shuffle.service.port=${SPARK_SHUFFLE_SERVICE_PORT}" \
--conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
"${SPARK_KAFKA_STREAM_JAR}" \
"${KAFKA_ADDRESSES}" "${TWITTER_BEARER_TOKEN}"

# Useful for logging memory usage
#--conf 'spark.driver.extraJavaOptions=-verbose:gc -Xlog:gc* -Xlog:gc+heap=trace' \
#--conf 'spark.executor.extraJavaOptions=-verbose:gc -Xlog:gc* -Xlog:gc+heap=trace' \

#--conf 'spark.driver.userClassPathFirst=true' \
