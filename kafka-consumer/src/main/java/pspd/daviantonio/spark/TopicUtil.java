package pspd.daviantonio.spark;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

public class TopicUtil {

	public static final String INPUT_TOPIC_NAME = "words";

	public static final String OUTPUT_WORD6_TOPIC_NAME = "words-length-6";
	public static final String OUTPUT_WORD8_TOPIC_NAME = "words-length-8";
	public static final String OUTPUT_WORD11_TOPIC_NAME = "words-length-11";

	public static final String OUTPUT_WORD_BEGIN_S_TOPIC_NAME = "words-begin-s";
	public static final String OUTPUT_WORD_BEGIN_P_TOPIC_NAME = "words-begin-p";
	public static final String OUTPUT_WORD_BEGIN_R_TOPIC_NAME = "words-begin-r";

	public static final String OUTPUT_COUNTS_PER_WORD_TOPIC_NAME = "counts-per-words";
	public static final String OUTPUT_WORD_COUNT_TOPIC_NAME = "word-count";

	public static final List<String> usedTopics = List.of(INPUT_TOPIC_NAME, OUTPUT_WORD6_TOPIC_NAME,
			OUTPUT_WORD8_TOPIC_NAME, OUTPUT_WORD11_TOPIC_NAME, OUTPUT_WORD_BEGIN_S_TOPIC_NAME,
			OUTPUT_WORD_BEGIN_P_TOPIC_NAME, OUTPUT_WORD_BEGIN_R_TOPIC_NAME, OUTPUT_COUNTS_PER_WORD_TOPIC_NAME,
			OUTPUT_WORD_COUNT_TOPIC_NAME);

	static int createTopicIfAbsent(final String topicName, Properties adminProps)
			throws InterruptedException, ExecutionException {
		try (Admin admin = Admin.create(adminProps)) {
			System.out.println("Creating topic " + topicName);

			// check if topic already exists
			Set<String> topicNames = admin.listTopics().names().get();
			if (!topicNames.contains(topicName)) {
				Integer partitions = 1;
				Short replicas = 1;

				NewTopic newTopic = new NewTopic(topicName, partitions, replicas);
				CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));

				KafkaFuture<Void> kafkaFuture = result.values().get(topicName);

				// wait for the creation
				kafkaFuture.get();

				System.out.println("Topic " + topicName + " created");

				return 0;
			}

			return 1;
		}
	}

	static int createTopicResetIfExists(final String topicName, Properties adminProps)
			throws InterruptedException, ExecutionException {
		try (Admin admin = Admin.create(adminProps)) {
			// check if topic already exists
			Set<String> topicNames = admin.listTopics().names().get();
			if (topicNames.contains(topicName)) {
				System.out.println("Topic " + topicName + " already exists, deleting it");

				DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topicName));
				KafkaFuture<Void> kafkaFuture = result.topicNameValues().get(topicName);

				// wait for deletion
				kafkaFuture.get();

				while (admin.listTopics().names().get().contains(topicName)) {
					System.out.println("Topic " + topicName + " still exists, waiting for deletion");
					TimeUnit.SECONDS.sleep(1);
				}

				System.out.println("Topic " + topicName + " deleted");
			}

			return createTopicIfAbsent(topicName, adminProps);
		}
	}

	static int createTopicsIfAbsent(List<String> topicNames, Properties adminProps)
			throws InterruptedException, ExecutionException {
		int result = 0;
		for (String name : topicNames) {
			result += createTopicIfAbsent(name, adminProps);
		}
		return result;
	}

	static int createTopicsResetIfExists(List<String> topicNames, Properties adminProps)
			throws InterruptedException, ExecutionException {
		int result = 0;
		for (String name : topicNames) {
			result += createTopicResetIfExists(name, adminProps);
		}
		return result;
	}

}
