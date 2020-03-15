package spark;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.*;
import emulator.ConfigManager;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;



/**
 * Spark Receiver for pubsub queue.
 *
 * @link https://spark.apache.org/docs/latest/streaming-custom-receivers.html
 * @link https://spark.apache.org/docs/latest/streaming-programming-guide.html#receiver-reliability
 * @link https://github.com/GoogleCloudPlatform/spark-examples
 * @link https://venkateshiyer.net/spark-streaming-custom-receiver-for-google-pubsub-3dc9d4a4451e
 */
public class CloudPubsubReceiver extends Receiver<String> {
    private String topicFullName;
    private String subscriptionFullName;
    private ConfigManager configManager;
    private boolean emulated;

    public CloudPubsubReceiver(String projectName, String topicName, String subscriptionName, StorageLevel storageLevel, boolean emulated) throws FileNotFoundException {
        super(storageLevel);
        String projectFullName = "projects/" + projectName;
        this.topicFullName = projectFullName + "/topics/" + topicName;
        this.subscriptionFullName = projectFullName + "/subscriptions/" + subscriptionName;
        this.configManager = ConfigManager.getInstance();
        this.emulated = emulated;
    }

    public CloudPubsubReceiver(String projectName, String topicName, String subscriptionName) throws FileNotFoundException {
        this(projectName, topicName, subscriptionName, StorageLevel.MEMORY_ONLY(), true);
    }

    public void onStart() {
        Pubsub client = emulated ? createEmulatorClient() : createAuthorizedClient();
        Subscription subscription = new Subscription().setTopic(topicFullName);
        try {
            // Create a subscription if it does not exist.
            client.projects().subscriptions().create(subscriptionFullName, subscription).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getDetails().getCode() != HttpURLConnection.HTTP_CONFLICT) {
                reportSubscriptionCreationError(e);
            }
        } catch (IOException e) {
            reportSubscriptionCreationError(e);
        }

        new Thread(this::receive).start();
    }

    public void onStop() {
        // Delete the subscription
        try {
            Pubsub client = emulated ? createEmulatorClient() : createAuthorizedClient();
            client.projects().subscriptions().delete(subscriptionFullName).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getDetails().getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                reportSubscriptionDeleteionError(e);
            }
            // Subscription may has already been deleted, but that's the expected behavior
            // with multiple receivers.
        } catch (IOException e) {
            reportSubscriptionDeleteionError(e);
        }
    }

    // Pull messages from Pubsub and store as RDD.
    private void receive() {
        Pubsub client = emulated ? createEmulatorClient() : createAuthorizedClient();
        if (client == null) {
            stop("Cannot create authorized client");
            return;
        }

        // Maximum # of messages in each Pub/sub Pull request
        int BATCH_SIZE = 500;
        int HTTP_TOO_MANY_REQUESTS = 429;

        // Backoff time when pubsub is throttled
        int MIN_BACKOFF_SECONDS = 1;
        int MAX_BACKOFF_SECONDS = 64;

        PullRequest pullRequest = new PullRequest().setReturnImmediately(false).setMaxMessages(BATCH_SIZE);
        int backoffTimeSeconds = MIN_BACKOFF_SECONDS;
        do {
            try {
                PullResponse pullResponse =
                        client.projects().subscriptions().pull(subscriptionFullName, pullRequest).execute();

                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessages();
                if (receivedMessages != null) {
                    // Store the message contents in batch
                    store(
                            receivedMessages
                                    .stream()
                                    .filter(m -> m.getMessage() != null && m.getMessage().getData() != null)
                                    .map(m -> new String(m.getMessage().decodeData(), StandardCharsets.UTF_8))
                                    .iterator());

                    AcknowledgeRequest ackRequest = new AcknowledgeRequest();
                    ackRequest.setAckIds(
                            receivedMessages
                                    .stream()
                                    .map(ReceivedMessage::getAckId)
                                    .collect(Collectors.toList()));
                    client.projects().subscriptions().acknowledge(subscriptionFullName, ackRequest).execute();
                    // Reset backoff time
                    backoffTimeSeconds = MIN_BACKOFF_SECONDS;
                }
            } catch (GoogleJsonResponseException e) {
                if (e.getDetails().getCode() == HTTP_TOO_MANY_REQUESTS) {
                    // When PubSub is rate throttled, retry with exponential backoff.
                    reportError(
                            "Reading from subscription "
                                    + subscriptionFullName
                                    + " is throttled. Will retry after "
                                    + backoffTimeSeconds
                                    + " seconds.",
                            e);
                    wait(backoffTimeSeconds);

                    backoffTimeSeconds = Math.min(backoffTimeSeconds << 1, MAX_BACKOFF_SECONDS);
                } else {
                    reportReadError(e);
                }
            } catch (IOException e) {
                reportReadError(e);
            }
        } while (!isStopped());
    }

    private Pubsub createAuthorizedClient() {
        try {
            // Create the credential
            HttpTransport httpTransport = Utils.getDefaultTransport();
            JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
            GoogleCredential credential =
                    GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);

            if (credential.createScopedRequired()) {
                credential = credential.createScoped(PubsubScopes.all());
            }
            HttpRequestInitializer initializer = new RetryHttpInitializerWrapper(credential);
            return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
                    .setApplicationName(configManager.getAppName())
                    .build();
        } catch (IOException e) {
            return createEmulatorClient();
        }
    }

    private Pubsub createEmulatorClient() {
        HttpTransport httpTransport = Utils.getDefaultTransport();
        JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
        return new Pubsub.Builder(httpTransport, jsonFactory, null)
                .setApplicationName(configManager.getAppName())
                .setSuppressAllChecks(true)
                .setRootUrl("http://" + configManager.getEmulatorHost())
                .build();
    }

    private void reportSubscriptionCreationError(Throwable e) {
        stop("Unable to create subscription: " + subscriptionFullName + " for topic " + topicFullName, e);
    }

    private void reportReadError(Throwable e) {
        stop("Unable to read subscription: " + subscriptionFullName, e);
    }

    private void reportSubscriptionDeleteionError(Throwable e) {
        reportError("Unable to delete subscription: " + subscriptionFullName, e);
    }

    private void wait(int backoffTimeSeconds) {
        try {
            Thread.sleep(1000 * backoffTimeSeconds);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
