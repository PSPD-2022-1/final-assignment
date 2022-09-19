package pspd.spark;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class WordConsumer {
	private static final String TO_KAFKA_WORD_COUNT_QUERY_NAME = "toKafkaWordCount";
	private static final String TO_KAFKA_COUNTS_PER_WORD_QUERY_NAME = "toKafkaCountsPerWord";
	private static final long TIMEOUT_MS = 1L * 60L * 1000L;
	private static final int PARTITIONS = 1000;

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {
		final String kafkaAddresses = args[0];
		final String checkpointLocation = args[1];
		SparkSession sparkSession = SparkSession.builder().appName("Word Consumer").getOrCreate();

		System.out.println("Reading from topic");
		Dataset<Row> tweetsWords = sparkSession.readStream().format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("subscribe", TopicUtil.WORDS_TOPIC_NAME)
				.option("startingOffsets", "earliest").load().selectExpr("CAST(key AS STRING) AS topicKey",
						"CAST(value AS STRING) AS topicValue", "topic", "partition", "offset", "timestamp",
						"timestampType");
		tweetsWords.repartition(PARTITIONS).createOrReplaceTempView("words");
		System.out.println("Read from topic");

		System.out.println("Printing on console");
		StreamingQuery consoleTweets = tweetsWords.writeStream().queryName("consoleTweets").outputMode(OutputMode.Update())
				.format("console").start();

		// counts per word
		Dataset<Row> countsPerWord = sparkSession
				.sql("SELECT topicValue, COUNT(topicValue) AS count FROM words GROUP BY topicValue");

		StreamingQuery consoleCountsPerWord = countsPerWord.writeStream().queryName("consoleCountsPerWord")
				.outputMode(OutputMode.Update()).format("console").start();

		StreamingQuery toKafkaCountsPerWord = countsPerWord
				.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value").writeStream()
				.queryName(TO_KAFKA_COUNTS_PER_WORD_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses)
				.option("topic", TopicUtil.OUTPUT_COUNTS_PER_WORD_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + TO_KAFKA_COUNTS_PER_WORD_QUERY_NAME).start();

		// global count
		Dataset<Row> wordCount = sparkSession.sql("SELECT COUNT(topicValue) AS count FROM words");
		StreamingQuery consoleWordCount = wordCount.repartition(PARTITIONS).writeStream().queryName("consoleWordCount")
				.outputMode(OutputMode.Update()).format("console").start();

		StreamingQuery toKafkaWordCount = wordCount.selectExpr("CAST(count AS STRING) AS value").repartition(PARTITIONS)
				.writeStream().queryName(TO_KAFKA_WORD_COUNT_QUERY_NAME).outputMode(OutputMode.Complete())
				.format("kafka").option("kafka.bootstrap.servers", kafkaAddresses)
				.option("topic", TopicUtil.OUTPUT_WORD_COUNT_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + TO_KAFKA_WORD_COUNT_QUERY_NAME).start();

		// wait for any termination or time out
		sparkSession.streams().awaitAnyTermination(TIMEOUT_MS);

		// terminate
		consoleTweets.stop();

		consoleCountsPerWord.stop();
		consoleWordCount.stop();

		toKafkaCountsPerWord.stop();
		toKafkaWordCount.stop();

		System.out.println("Closing session");
		sparkSession.close();
		System.out.println("Done");
	}
}
