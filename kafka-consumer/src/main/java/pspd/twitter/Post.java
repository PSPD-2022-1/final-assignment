package pspd.twitter;

public class Post {
	private String id;
	private String text;

	public Post(String id, String text) {
		super();
		this.id = id;
		this.text = text;
	}

	public String getId() {
		return id;
	}

	public String getText() {
		return text;
	}
}
