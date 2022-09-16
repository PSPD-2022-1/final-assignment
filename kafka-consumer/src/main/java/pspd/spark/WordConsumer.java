package pspd.spark;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class WordConsumer {
	private static final String TO_KAFKA_WORD_COUNT_QUERY_NAME = "toKafkaWordCount";
	private static final String TO_KAFKA_COUNTS_PER_WORD_QUERY_NAME = "toKafkaCountsPerWord";
	private static final String WORD_LENGTH11_KAFKA_QUERY_NAME = "toKafkaLength11Words";
	private static final String WORD_LENGTH8_KAFKA_QUERY_NAME = "toKafkaLength8Words";
	private static final String WORD_LENGTH6_KAFKA_QUERY_NAME = "toKafkaLength6Words";
	private static final String RWORDS_KAFKA_QUERY_NAME = "toKafkaRWords";
	private static final String PWORDS_KAFKA_QUERY_NAME = "toKafkaPWords";
	private static final String SWORDS_KAFKA_QUERY_NAME = "toKafkaSWords";
	private static final long TIMEOUT_MS = 25L * 60L * 1000L;
	private static final int PARTITIONS = 1000;

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {
		final String kafkaAddresses = args[0];
		final String checkpointLocation = args[1];
		SparkSession sparkSession = SparkSession.builder().appName("Word Consumer").getOrCreate();

		System.out.println("Reading from topic");
		Dataset<Row> caughtWords = sparkSession.readStream().format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("subscribe", TopicUtil.INPUT_TOPIC_NAME)
				.option("startingOffsets", "earliest").load().selectExpr("CAST(key AS STRING) AS topicKey",
						"CAST(value AS STRING) AS topicValue", "topic", "partition", "offset", "timestamp",
						"timestampType");
		caughtWords.repartition(PARTITIONS).createOrReplaceTempView("words");
		System.out.println("Read from topic");

		System.out.println("Printing on console");
		StreamingQuery consoleWords = caughtWords.writeStream().outputMode(OutputMode.Update()).format("console")
				.start();

		// words that start with 's'
		Dataset<Row> sWords = caughtWords.where("topicValue LIKE 's%'")
				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();
		sWords.createOrReplaceTempView("sWords");

		StreamingQuery consoleSWords = sWords.writeStream().queryName("consoleSWords").outputMode(OutputMode.Update())
				.format("console").start();

		StreamingQuery toKafkaSWords = sWords.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value")
				.writeStream().queryName(SWORDS_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses)
				.option("topic", TopicUtil.OUTPUT_WORD_BEGIN_S_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + SWORDS_KAFKA_QUERY_NAME).start();

		// words that start with 'p'
		Dataset<Row> pWords = caughtWords.where("topicValue LIKE 'p%'")
				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();

		StreamingQuery consolePWords = pWords.writeStream().queryName("consolePWords").outputMode(OutputMode.Update())
				.format("console").start();

		StreamingQuery toKafkaPWords = pWords.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value")
				.writeStream().queryName(PWORDS_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses)
				.option("topic", TopicUtil.OUTPUT_WORD_BEGIN_P_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + PWORDS_KAFKA_QUERY_NAME).start();

		// words that start with 'r'
		Dataset<Row> rWords = caughtWords.where("topicValue LIKE 'r%'")
				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();

		StreamingQuery consoleRWords = rWords.writeStream().queryName("consoleRWords").outputMode(OutputMode.Update())
				.format("console").start();

		StreamingQuery toKafkaRWords = rWords.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value")
				.writeStream().queryName(RWORDS_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses)
				.option("topic", TopicUtil.OUTPUT_WORD_BEGIN_R_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + RWORDS_KAFKA_QUERY_NAME).start();

		// words with 6 characters
		Dataset<Row> length6Words = caughtWords.where("CHAR_LENGTH(topicValue) == 6")
				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();

		StreamingQuery consoleLength6Words = length6Words.writeStream().queryName("consoleLength6Words")
				.outputMode(OutputMode.Update()).format("console").start();

		StreamingQuery toKafkaLength6Words = length6Words
				.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value").writeStream()
				.queryName(WORD_LENGTH6_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", TopicUtil.OUTPUT_WORD6_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + WORD_LENGTH6_KAFKA_QUERY_NAME).start();

		// words with 8 characters
		Dataset<Row> lenth8Words = caughtWords.where("CHAR_LENGTH(topicValue) == 8")
				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();

		StreamingQuery consoleLength8Words = lenth8Words.writeStream().queryName("consoleLength8Words")
				.outputMode(OutputMode.Update()).format("console").start();

		StreamingQuery toKafkaLength8Words = lenth8Words
				.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value").writeStream()
				.queryName(WORD_LENGTH8_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", TopicUtil.OUTPUT_WORD8_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + WORD_LENGTH8_KAFKA_QUERY_NAME).start();

		// words with 11 characters
		Dataset<Row> length11Words = caughtWords.where("CHAR_LENGTH(topicValue) == 11")

				.groupBy(functions.window(caughtWords.col("timestamp"), "3 seconds"), caughtWords.col("topicValue"))
				.count();

		StreamingQuery consoleLength11Words = length11Words.writeStream().queryName("consoleLength11Words")
				.outputMode(OutputMode.Update()).format("console").start();

		StreamingQuery toKafkaLength11Words = length11Words
				.selectExpr("topicValue AS key", "CAST(count AS STRING) AS value").writeStream()
				.queryName(WORD_LENGTH11_KAFKA_QUERY_NAME).outputMode(OutputMode.Complete()).format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", TopicUtil.OUTPUT_WORD11_TOPIC_NAME)
				.option("checkpointLocation", checkpointLocation + "/" + WORD_LENGTH11_KAFKA_QUERY_NAME).start();

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
		consoleWords.stop();

		consoleSWords.stop();
		consolePWords.stop();
		consoleRWords.stop();

		consoleLength6Words.stop();
		consoleLength8Words.stop();
		consoleLength11Words.stop();

		consoleCountsPerWord.stop();
		consoleWordCount.stop();

		toKafkaSWords.stop();
		toKafkaPWords.stop();
		toKafkaRWords.stop();

		toKafkaLength6Words.stop();
		toKafkaLength8Words.stop();
		toKafkaLength11Words.stop();

		toKafkaCountsPerWord.stop();
		toKafkaWordCount.stop();

		System.out.println("Closing session");
		sparkSession.close();
		System.out.println("Done");
	}
}
