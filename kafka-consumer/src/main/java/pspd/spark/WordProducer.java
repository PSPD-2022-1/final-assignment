package pspd.spark;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class WordProducer {
	private static final String KAFKA_INPUT = TopicUtil.INPUT_TOPIC_NAME;

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final String kafkaAddresses = args[0];
		final String inputFile = args[1];

		Properties props = new Properties();

		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddresses);

		for (String topic : TopicUtil.usedTopics) {
			System.out.println("Will create topic " + topic);
			TopicUtil.createTopicIfAbsent(topic, props);
		}

		SparkSession sparkSession = SparkSession.builder().appName("Word Producer").getOrCreate();

		System.out.println("Reading file " + inputFile);
		Dataset<String> text = sparkSession.read().textFile(inputFile);
		System.out.println("Read file " + inputFile);

		final String regex = "\\w+";
		final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE | Pattern.UNICODE_CHARACTER_CLASS);

		System.out.println("Generating words from file");
		Dataset<String> textwords = text.flatMap((FlatMapFunction<String, String>) textStr -> {
			return pattern.matcher(textStr).results().map((MatchResult matchGroup) -> {
				return matchGroup.group();
			}).iterator();
		}, Encoders.STRING());
		System.out.println("Generated words from file");

		System.out.println("Writing to kafka");
		textwords.selectExpr("CAST(value AS STRING)").write().format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", KAFKA_INPUT).save();

		System.out.println("Closing session");
		sparkSession.close();
		System.out.println("Done");
	}
}
