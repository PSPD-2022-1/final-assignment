package pspd.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.twitter.clientlib.model.FilteredStreamingTweetResponse;
import com.twitter.clientlib.model.RuleNoId;
import com.twitter.clientlib.model.Tweet;

import pspd.twitter.RecentTweetSearcher;

public class WordProducer {
	private static final String TWEETS_TOPIC_NAME = TopicUtil.TWEETS_TOPIC_NAME;
	private static final String WORDS_TOPIC_NAME = TopicUtil.WORDS_TOPIC_NAME;

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final String kafkaAddresses = args[0];
		final String twitterBearerToken = args[1];

		Properties props = new Properties();

		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddresses);

		for (String topic : TopicUtil.usedTopics) {
			System.out.println("Will create topic " + topic);
			TopicUtil.createTopicIfAbsent(topic, props);
		}

		SparkSession sparkSession = SparkSession.builder().appName("Tweets Producer").getOrCreate();

		List<RuleNoId> rules = new ArrayList<RuleNoId>();

		rules.add(new RuleNoId().tag("matchesHashTagBrasil").value("#Brasil"));
		rules.add(new RuleNoId().tag("matchesHashTagEleições2022").value("#Eleições2022"));

		Stream<FilteredStreamingTweetResponse> jsonSTweettream = RecentTweetSearcher.getTweetStream(twitterBearerToken,
				rules);

		List<FilteredStreamingTweetResponse> jsonTweets;
		if (jsonSTweettream == null) {
			System.out.println("Empty stream");
			System.out.println("Falling back to recent tweets");

			String query = "";

			query += rules.get(0);

			for (int i = 1; i < rules.size(); i++) {
				query += " or " + rules.get(i);
			}

			System.out.println("Query rule: '" + query + "'");

			jsonTweets = RecentTweetSearcher.getTweets(twitterBearerToken, query).stream().map((Tweet tweet) -> {
				FilteredStreamingTweetResponse f = new FilteredStreamingTweetResponse();
				f.setData(tweet);

				return f;
			}).collect(Collectors.toList());
		} else {
			jsonTweets = jsonSTweettream.limit(5).collect(Collectors.toList());
		}

		Dataset<String> tweets = sparkSession.createDataset(
				jsonTweets.stream().map(tweet -> tweet.toJson()).collect(Collectors.toList()), Encoders.STRING());
		tweets.selectExpr("CAST(value AS STRING)").write().format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", TWEETS_TOPIC_NAME).save();

		final String regex = "\\w+";
		final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE | Pattern.UNICODE_CHARACTER_CLASS);

		System.out.println("Generating words");

		List<StructField> fields = new ArrayList<>();

		fields.add(DataTypes.createStructField("key", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("value", DataTypes.StringType, false));

		StructType structType = DataTypes.createStructType(fields);

		List<Row> wordRows = new ArrayList<Row>();
		jsonTweets.forEach((FilteredStreamingTweetResponse tweet) -> {
			String id = tweet.getData().getId();
			String text = tweet.getData().getText();

			pattern.matcher(text).results().map((MatchResult matchGroup) -> {
				return RowFactory.create(id, matchGroup.group());
			}).forEach((Row r) -> {
				wordRows.add(r);
			});
		});

		Dataset<Row> tweetWords = sparkSession.createDataFrame(wordRows, structType);
		System.out.println("Generated words");

		System.out.println("Writing to kafka");
		tweetWords.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write().format("kafka")
				.option("kafka.bootstrap.servers", kafkaAddresses).option("topic", WORDS_TOPIC_NAME).save();

		System.out.println("Closing session");
		sparkSession.close();
		System.out.println("Done");
	}
}
