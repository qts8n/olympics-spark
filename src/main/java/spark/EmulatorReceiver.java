package spark;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import io.grpc.ManagedChannel;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import pubsub.EmulatorPublisher;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
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
    public EmulatorReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    public EmulatorReceiver() {
        this(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
    }

    private void receiveBatch() {
        ManagedChannel channel = null;

        try {
            channel = EmulatorPublisher.getChannel();
            TransportChannelProvider channelProvider = EmulatorPublisher.createProvider(channel);
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

            SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings);

            ProjectSubscriptionName subscription = EmulatorPublisher.getSubscription();

            PullRequest pullRequest = PullRequest.newBuilder()
                    .setReturnImmediately(false)
                    .setMaxMessages(50)
                    .setSubscription(subscription.toString())
                    .build();
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
            List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

            if (receivedMessages != null && !receivedMessages.isEmpty()) {
                store(receivedMessages.stream()
                        .filter(m -> m != null && m.getMessage() != null && m.getMessage().getData() != null)
                        .map(m -> m.getMessage().getData().toStringUtf8())
                        .filter(Objects::nonNull)
                        .iterator());

                AcknowledgeRequest ackRequest = AcknowledgeRequest.newBuilder()
                        .setSubscription(subscription.toString())
                        .addAllAckIds(receivedMessages
                                .stream()
                                .map(ReceivedMessage::getAckId)
                                .collect(Collectors.toList()))
                        .build();
                subscriber.acknowledgeCallable().call(ackRequest);
            }
        } catch (ApiException exception) {
            reportError("Could not load messages from queue", exception);
        } catch (IOException exception) {
            stop("Unable to receive messages", exception);
        } catch (Exception exception) {
            stop("Something went wrong", exception);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    private void receive() {
        do {
            receiveBatch();
        } while (!isStopped());
    }
}
