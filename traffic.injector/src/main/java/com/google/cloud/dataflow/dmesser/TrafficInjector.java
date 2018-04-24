package com.google.cloud.dataflow.dmesser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

/**
 * A streaming injector for News sources using Pubsub I/O.
 *
 * <p>
 * This pipeline example pulls top News stories from the web and publishes them
 * to two corresponding PubSub topics.
 * </p>
 *
 * <p>
 * To run this example using the Dataflow service, you must provide an output
 * pubsub topic for news, using the {@literal --inputTopic} option. This
 * injector can be run locally using the direct runner.
 * </p>
 * E.g.: java -cp target/examples-1.jar \
 * com.google.cloud.dataflow.examples.StockInjector \
 * --runner=DirectPipelineRunner \ --project=google.com:clouddfe \
 * --stagingLocation=gs://clouddfe-test/staging-$USER \
 * --outputTopic=/topics/google.com:clouddfe/stocks1w1
 */

public class TrafficInjector {

	private String projectName;
	private String topic;
	private String apiURL;

	private Logger logger = Logger.getLogger(this.getClass().getName());

	/**
	 * A constructor of TrafficInjector.
	 */
	public TrafficInjector(String projectName, String topic, String apiURL) {
		this.projectName = projectName;
		this.topic = topic;
		this.apiURL = apiURL;
	}

	/**
	 * Fetches the news from the specified URL and returns the titles.
	 */
	public String getTrafficFlow() {

		String apiURL = this.apiURL;
		String trafficFlow = "";

		try {
			URL url = new URL(apiURL);
			HttpsURLConnection con = (HttpsURLConnection) url.openConnection();			
			
			final int code = con.getResponseCode();
			
			if (code != 200) {
				throw new IOException("Unexpected response code: expected 200, got: " + code);
			}

			BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));

			String line = "";

			while ((line = reader.readLine()) != null) {
				trafficFlow += line + "\n";
			}

			reader.close();
		} catch (MalformedURLException e) {
			logger.severe("URL malformed" + apiURL + ":\n" + e.getMessage());
		} catch (IOException e) {
			logger.severe("IO error occured in trying to reach " + apiURL + ":\n" + e.getMessage());
		}

		return trafficFlow;
	}

	/**
	 * Fetches the traffic flow and publishes it.
	 * 
	 * @throws Exception
	 */
	public void publishTrafficFlow() throws Exception {
		String trafficFlow = getTrafficFlow();
		publishMessage(trafficFlow);
	}

	/**
	 * Publishes the given message to the given topic.
	 * 
	 * @throws Exception
	 */
	public void publishMessage(String message) throws Exception {
		int maxLogMessageLength = 200;
		if (message.length() < maxLogMessageLength) {
			maxLogMessageLength = message.length();
		}
		logger.info("Publishing ...." + message.substring(0, maxLogMessageLength));

		ProjectTopicName topicName = ProjectTopicName.of(this.projectName, this.topic);
		Publisher publisher = null;
		ApiFuture<String> messageIdFuture = null;

		try {
			publisher = Publisher.newBuilder(topicName).build();
			ByteString data = ByteString.copyFromUtf8(message);
			PubsubMessage pubSubMessage = PubsubMessage.newBuilder().setData(data).build();

			messageIdFuture = publisher.publish(pubSubMessage);
		} catch (IOException e) {
			logger.severe("IOException when trying to send message:\n" + e.getMessage());
		} finally {
			if (messageIdFuture != null) {
				// wait on any pending publish requests.
				String messageId = messageIdFuture.get();
				logger.info("published with message ID: " + messageId);
			}

			if (publisher != null) {
				// When finished with the publisher, shutdown to free up resources.
				publisher.shutdown();
			}
		}
	}

	/**
	 * Fetches TrafficFlow and publishes it to the specified Cloud Pub/Sub topic.
	 */
	public static void main(String[] args) throws Exception {
		// Get options from command-line.
		if (args.length < 3) {
			System.out.println("Please specify the output Pubsub topic");
			return;
		} else if (args.length < 2) {
			System.out.println("Please specify the project ID");
			return;
		} else if (args.length < 1) {
			System.out.println("Please specify the traffic flow API URL");
			return;
		}

		String url = new String(args[0]);
		String project = new String(args[1]);
		String topic = new String(args[2]);

		System.out.println("Traffic Flow API: " + url);
		System.out.println("Output Pubsub project: " + project);
		System.out.println("Output Pubsub topic: " + topic);
		

		TrafficInjector injector = new TrafficInjector(project, topic, url);

		while (true) {
			injector.publishTrafficFlow();

			try {
				// thread to sleep for 5 minutes
				Thread.sleep(300000);
			} catch (java.lang.InterruptedException ie) {
				;
			}
		}
	}
}
