#!/bin/bash
set -e

source 'env-vars.sh'

# output
fail_if_unset_or_empty "HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR"
fail_if_unset_or_empty "HDFS_BIN"
fail_if_unset_or_empty "TOPICS"
fail_if_unset_or_empty "KAFKA_TOPICS_BIN"
fail_if_unset_or_empty "KAFKA_ADDRESSES"

echo "Removing old ${HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR} from HDFS"
"${HDFS_BIN}" dfs -rm -r "${HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR}"
echo ""

echo "Recreating ${HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR} from HDFS"
"${HDFS_BIN}" dfs -mkdir -p "${HDFS_SPARK_KAFKA_STREAM_OUTFILE_DIR}"
echo ""

for topic in "${TOPICS[@]}"; do
	echo "Deleting topic ${topic}"
	"${KAFKA_TOPICS_BIN}" --bootstrap-server "${KAFKA_ADDRESSES}" \
		--delete --topic "${topic}"
done

for topic in "${TOPICS[@]}"; do
	echo "Creating topic ${topic}"
	"${KAFKA_TOPICS_BIN}" --bootstrap-server "${KAFKA_ADDRESSES}" \
		--create --topic "${topic}"
done
