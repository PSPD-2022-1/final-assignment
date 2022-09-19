#!/bin/bash

source 'env-vars.sh'

for topic in "${TOPICS[@]}"; do
	echo "Now watching topic '${topic}'"
	"${KAFKA_CONSUMER_BIN}" --bootstrap-server "${KAFKA_ADDRESSES}" \
		--topic "${topic}" --from-beginning \
		--property 'print.key=true' \
		--property 'print.value=true' \
		--property 'print.offset=true' \
		--property 'print.timestamp=true' \
		--property 'print.partition=true' \
		--property 'print.headers=true'
done
