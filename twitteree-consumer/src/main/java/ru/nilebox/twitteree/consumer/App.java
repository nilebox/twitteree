package ru.nilebox.twitteree.consumer;

/**
 * Entry
 * point
 *
 */
public class App {

	public static void main(String[] args) {
		try {
			String key = "";
			String keySecret = "";
			String token = "";
			String tokenSecret = "";
			
			int timeout = 10000;
			if (args != null && args.length > 0) {
				try {
					timeout = Integer.parseInt(args[0]);
				} catch (Exception ex) {
					System.out.println(ex);
				}
			}
			
			TwitterClient.oauth(key, keySecret, token, tokenSecret, timeout);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
