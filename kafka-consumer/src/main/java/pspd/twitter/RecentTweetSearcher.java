package pspd.twitter;

import java.util.ArrayList;
import java.util.List;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.Tweet;

public class RecentTweetSearcher {

	static List<Post> getPosts(String query) {
		List<Post> posts = new ArrayList<Post>();

		String twitterBearerToken = System.getenv("TWITTER_BEARER_TOKEN");
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
			System.err.println("Status code: " + e.getCode());
			System.err.println("Reason: " + e.getResponseBody());
			System.err.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}

		return posts;
	}

	static void printPosts(List<Post> posts) {
		for (Post p : posts) {
			System.out.println("id=" + p.getId() + " " + "text='" + p.getText() + "'");
		}
	}

	public static void main(String[] args) throws ApiException {
		List<Post> posts = RecentTweetSearcher.getPosts("#Brasil");
		RecentTweetSearcher.printPosts(posts);
	}
}
