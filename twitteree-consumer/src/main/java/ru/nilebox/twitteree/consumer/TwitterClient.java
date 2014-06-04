/**
 * Copyright
 * 2013
 * Twitter,
 * Inc.
 * Licensed
 * under
 * the
 * Apache
 * License,
 * Version
 * 2.0
 * (the
 * "License");
 * you
 * may
 * not
 * use
 * this
 * file
 * except
 * in
 * compliance
 * with
 * the
 * License.
 * You
 * may
 * obtain
 * a
 * copy
 * of
 * the
 * License
 * at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless
 * required
 * by
 * applicable
 * law
 * or
 * agreed
 * to
 * in
 * writing,
 * software
 * distributed
 * under
 * the
 * License
 * is
 * distributed
 * on
 * an
 * "AS
 * IS"
 * BASIS,
 * WITHOUT
 * WARRANTIES
 * OR
 * CONDITIONS
 * OF
 * ANY
 * KIND,
 * either
 * express
 * or
 * implied.
 * See
 * the
 * License
 * for
 * the
 * specific
 * language
 * governing
 * permissions
 * and
 * limitations
 * under
 * the
 * License.
 *
 */
package ru.nilebox.twitteree.consumer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient {

	public static void oauth(String consumerKey, String consumerSecret, String token, String secret, int timeout) throws InterruptedException {
		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.
		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);
		//endpoint.languages(Lists.newArrayList("en"));
		// add some track terms
	    //endpoint.trackTerms(Lists.newArrayList("#moscow"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
		//Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		BasicClient client = new ClientBuilder()
				.name("twitteree")
				.hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue))
				.build();

		// Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
		// calling the listeners on each message
		int numProcessingThreads = 4;
		ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
				client, queue, Lists.newArrayList(new StatusHandler()), service);

		// Establish a connection
		t4jClient.connect();
		for (int threads = 0; threads < numProcessingThreads; threads++) {
			// This must be called once per processing thread
			t4jClient.process();
		}
		
		// Give time to retrieve some twitter messages
		Thread.sleep(timeout);

		client.stop();

		// Print some stats
		System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}
	
}
