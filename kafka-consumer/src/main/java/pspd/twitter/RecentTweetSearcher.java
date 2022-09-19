package pspd.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.AddOrDeleteRulesRequest;
import com.twitter.clientlib.model.AddOrDeleteRulesResponse;
import com.twitter.clientlib.model.AddRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequestDelete;
import com.twitter.clientlib.model.FilteredStreamingTweetResponse;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.Rule;
import com.twitter.clientlib.model.RuleNoId;
import com.twitter.clientlib.model.RulesLookupResponse;
import com.twitter.clientlib.model.Tweet;

public class RecentTweetSearcher {

	public static List<Post> getPosts(String query) {
		String twitterBearerToken = System.getenv("TWITTER_BEARER_TOKEN");

		return getPosts(twitterBearerToken, query);
	}

	public static List<Post> getPosts(String twitterBearerToken, String query) {
		List<Post> posts = new ArrayList<Post>();

		TwitterCredentialsBearer twitterCredentialsBearer = new TwitterCredentialsBearer(twitterBearerToken);
		TwitterApi apiInstance = new TwitterApi(twitterCredentialsBearer);
		Get2TweetsSearchRecentResponse response;
		try {
			response = apiInstance.tweets().tweetsRecentSearch(query).execute();

			List<Tweet> tweets = response.getData();

			tweets.forEach((tweet) -> {
				posts.add(new Post(tweet.getId(), tweet.getText()));
			});
		} catch (ApiException e) {
			System.out.println("Status code: " + e.getCode());
			System.out.println("Reason: " + e.getResponseBody());
			System.out.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}

		return posts;
	}

	public static List<Tweet> getTweets(String twitterBearerToken, String query) {
		List<Tweet> tweets = new ArrayList<Tweet>();

		TwitterCredentialsBearer twitterCredentialsBearer = new TwitterCredentialsBearer(twitterBearerToken);
		TwitterApi apiInstance = new TwitterApi(twitterCredentialsBearer);
		Get2TweetsSearchRecentResponse response;
		try {
			response = apiInstance.tweets().tweetsRecentSearch(query).execute();

			tweets = response.getData();
		} catch (ApiException e) {
			System.out.println("Status code: " + e.getCode());
			System.out.println("Reason: " + e.getResponseBody());
			System.out.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}

		return tweets;
	}

	public static void printPosts(List<Post> posts) {
		for (Post p : posts) {
			printPost(p);
		}
	}

	public static void printPost(Post p) {
		System.out.println("id=" + p.getId() + " " + "text='" + p.getText() + "'");
	}

	@javax.annotation.Nullable
	public static Stream<FilteredStreamingTweetResponse> getTweetStream(List<RuleNoId> rules) {
		String twitterBearerToken = System.getenv("TWITTER_BEARER_TOKEN");

		return getTweetStream(twitterBearerToken, rules);
	}

	@javax.annotation.Nullable
	public static Stream<FilteredStreamingTweetResponse> getTweetStream(String twitterBearerToken,
			List<RuleNoId> rules) {
		TwitterCredentialsBearer twitterCredentialsBearer = new TwitterCredentialsBearer(twitterBearerToken);
		TwitterApi apiInstance = new TwitterApi(twitterCredentialsBearer);

		RulesLookupResponse rulesLookup;
		try {
			rulesLookup = apiInstance.tweets().getRules().execute();

			List<Rule> currentRules = rulesLookup.getData();

			List<String> rulesIds = new ArrayList<String>();

			AddOrDeleteRulesRequest addOrDeleteRulesRequest = new AddOrDeleteRulesRequest();

			if (currentRules != null) {
				System.out.println("Deleting current rules");

				DeleteRulesRequest deleteRulesRequest = new DeleteRulesRequest();
				DeleteRulesRequestDelete rulesToDelete = new DeleteRulesRequestDelete();

				currentRules.forEach((rule) -> {
					rulesIds.add(rule.getId());
				});

				rulesToDelete.setIds(rulesIds);

				deleteRulesRequest.delete(rulesToDelete);

				addOrDeleteRulesRequest.setActualInstance(deleteRulesRequest);

				AddOrDeleteRulesResponse deleteResponse = apiInstance.tweets().addOrDeleteRules(addOrDeleteRulesRequest)
						.dryRun(true).execute();

				System.out.println(deleteResponse);

				deleteResponse = apiInstance.tweets().addOrDeleteRules(addOrDeleteRulesRequest).execute();

			} else {
				System.out.println("No rules previously defined");
			}

			AddRulesRequest addRulesRequest = new AddRulesRequest();

			addRulesRequest.add(rules);

			addOrDeleteRulesRequest.setActualInstance(addRulesRequest);

			AddOrDeleteRulesResponse addResponse = apiInstance.tweets().addOrDeleteRules(addOrDeleteRulesRequest)
					.dryRun(true).execute();

			System.out.println(addResponse);

			addResponse = apiInstance.tweets().addOrDeleteRules(addOrDeleteRulesRequest).execute();

			InputStream tweetStream = apiInstance.tweets().searchStream().execute(5);

			BufferedReader reader = new BufferedReader(new InputStreamReader(tweetStream));

			return reader.lines().map(json -> {
				try {
					return FilteredStreamingTweetResponse.fromJson(json);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}).filter(tweet -> {
				return tweet != null;
			});
		} catch (ApiException e) {
			System.out.println("Status code: " + e.getCode());
			System.out.println("Reason: " + e.getResponseBody());
			System.out.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}

		return null;
	}

	public static Post tweetToPost(FilteredStreamingTweetResponse tweet) {
		return new Post(tweet.getData().getId(), tweet.getData().getText());
	}

	public static void main(String[] args) throws Exception {
		System.err.println("RecentTweetSearcher.getPosts(\"#Brasil\")");
		List<Post> recentPosts = RecentTweetSearcher.getPosts("#Brasil");
		RecentTweetSearcher.printPosts(recentPosts);

		List<RuleNoId> rules = new ArrayList<RuleNoId>();

		rules.add(new RuleNoId().tag("matchesHashTagBrasil").value("#Brasil"));
		rules.add(new RuleNoId().tag("matchesHashTagEleições2022").value("#Eleições2022"));

		System.err.println("RecentTweetSearcher.getTweetStream()");
		Stream<FilteredStreamingTweetResponse> tweets = RecentTweetSearcher.getTweetStream(rules);

		if (tweets != null) {
			for (FilteredStreamingTweetResponse tweet : tweets.limit(5).collect(Collectors.toList())) {
				System.out.println(tweet);
				System.err.println("RecentTweetSearcher.tweetToPost()");
				RecentTweetSearcher.printPost(tweetToPost(tweet));
			}

			tweets.close();
		} else {
			System.err.println("Empty tweet stream");
		}
	}
}
