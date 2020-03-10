package spark;

import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import emulator.Emulator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


/**
 * Spark Receiver for a local pubsub emulator.
 *
 * @link https://spark.apache.org/docs/latest/streaming-custom-receivers.html
 * @link https://spark.apache.org/docs/latest/streaming-programming-guide.html#receiver-reliability
 * @link https://github.com/GoogleCloudPlatform/spark-examples
 * @link https://venkateshiyer.net/spark-streaming-custom-receiver-for-google-pubsub-3dc9d4a4451e
 */
public class EmulatorReceiver extends Receiver<String> {
    private Emulator emulator;

    public EmulatorReceiver(StorageLevel storageLevel) throws IOException {
        super(storageLevel);
        this.emulator = new Emulator();
    }

    public EmulatorReceiver() throws IOException {
        this(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        try {
            emulator.load();
        } catch (IOException | ExecutionException | InterruptedException | TimeoutException e) {
            reportReadError(e);
            stop(e.getMessage());
            return;
        }

        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        emulator.clean();
    }

    private void receive() {
        // Maximum # of messages in each Pub/sub Pull request
        int BATCH_SIZE = 500;

        // Backoff time when pubsub is throttled
        int MIN_BACKOFF_SECONDS = 1;
        int MAX_BACKOFF_SECONDS = 64;

        PullRequest pullRequest = PullRequest.newBuilder()
                .setReturnImmediately(false)
                .setMaxMessages(BATCH_SIZE)
                .setSubscription(emulator.getSubscription())
                .build();

        int backoffTimeSeconds = MIN_BACKOFF_SECONDS;
        do {
            try {
                PullResponse pullResponse = emulator.getSubscriber().pullCallable().call(pullRequest);
                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

                if (receivedMessages == null) continue;

                store(receivedMessages.stream()
                        .filter(m -> m.getMessage() != null &&
                                m.getMessage().getData() != null)
                        .map(m -> m.getMessage().getData().toStringUtf8())
                        .filter(Objects::nonNull)
                        .iterator());

                AcknowledgeRequest ackRequest = AcknowledgeRequest.newBuilder()
                        .setSubscription(emulator.getSubscription())
                        .addAllAckIds(receivedMessages
                                .stream()
                                .map(ReceivedMessage::getAckId)
                                .collect(Collectors.toList()))
                        .build();
                emulator.getSubscriber().acknowledgeCallable().call(ackRequest);
                // Reset backoff time
                backoffTimeSeconds = MIN_BACKOFF_SECONDS;
            } catch (Exception e) {
                reportReadError(e);
                wait(backoffTimeSeconds);
                backoffTimeSeconds = Math.min(backoffTimeSeconds << 1, MAX_BACKOFF_SECONDS);
            }
        } while (!isStopped());
    }

    private void reportReadError(Throwable e) {
        stop("Unable to read subscription: " + emulator.getSubscription() + ". Retrying...", e);
    }

    private void wait(int backoffTimeSeconds) {
        try {
            Thread.sleep(1000 * backoffTimeSeconds);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
