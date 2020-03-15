package spark;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import emulator.ConfigManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import pubsub.EmulatorPublisher;

import java.io.FileNotFoundException;
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
    public EmulatorReceiver(StorageLevel storageLevel) throws IOException {
        super(storageLevel);
    }

    public EmulatorReceiver() throws IOException {
        this(StorageLevel.MEMORY_ONLY());
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
    }

    private void receive() {
        ConfigManager configManager;
        try {
            configManager = ConfigManager.getInstance();
        } catch (FileNotFoundException e) {
            stop("No config file was found", e);
            return;
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(configManager.getEmulatorHost()).usePlaintext().build();
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        SubscriptionAdminClient client = null;
        Subscription subscription = null;
        try {
            SubscriptionAdminSettings subscriptionAdminSettings= SubscriptionAdminSettings.newBuilder()
                    .setCredentialsProvider(credentialsProvider)
                    .setTransportChannelProvider(channelProvider)
                    .build();
            client = SubscriptionAdminClient.create(subscriptionAdminSettings);
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(configManager.getProject(), configManager.getSubscription());

            subscription = client.createSubscription(
                    subscriptionName,
                    EmulatorPublisher.getTopic(),
                    PushConfig.getDefaultInstance(),
                    0);
        } catch (IOException ignored) {
            channel.shutdownNow();
            stop("Cannot create subscription with given credentials");
            return;
        } catch (ApiException exception) {
            if (exception.getStatusCode().getCode() != StatusCode.Code.ALREADY_EXISTS) {
                channel.shutdownNow();
                stop("Cannot create subscription", exception);
                return;
            }
        }

        SubscriberStub subscriber;
        try {
            SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
            subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
        } catch (IOException ignored) {
            channel.shutdownNow();
            stop("Cannot create subscriber with given credentials");
            return;
        }

        if (client == null || subscription == null) {
            stop("Could not create client or subscription");
            return;
        }

        PullRequest pullRequest = PullRequest.newBuilder()
                .setReturnImmediately(false)
                .setMaxMessages(configManager.getEmulatorMaxLine())
                .setSubscription(subscription.getName())
                .build();

        do {
            try {
                PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
                List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

                if (receivedMessages == null) continue;

                store(receivedMessages.stream()
                        .filter(m -> m != null && m.getMessage() != null && m.getMessage().getData() != null)
                        .map(m -> m.getMessage().getData().toStringUtf8())
                        .filter(Objects::nonNull)
                        .iterator());

                AcknowledgeRequest ackRequest = AcknowledgeRequest.newBuilder()
                        .setSubscription(configManager.getSubscription())
                        .addAllAckIds(receivedMessages
                                .stream()
                                .map(ReceivedMessage::getAckId)
                                .collect(Collectors.toList()))
                        .build();
                subscriber.acknowledgeCallable().call(ackRequest);
            } catch (Exception e) {
                stop("Unable to read subscription", e);
            }
        } while (!isStopped());

        client.deleteSubscription(configManager.getSubscription());
        channel.shutdownNow();
    }
}
