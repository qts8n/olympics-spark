package emulator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Publishes messages to the local instance of the Pub/Sub emulator.
 *
 * Java API to push and pull Pub/Sub messages.
 *
 * @link https://dzone.com/articles/pubsub-local-emulator
 */
public class Emulator {
    private ConfigManager configManager;

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;

    private TopicAdminClient topicAdmin;
    private ProjectTopicName topicName;
    private Publisher publisher;

    private SubscriptionAdminClient subscriptionAdminClient;
    private Subscription subscription;
    private SubscriberStub subscriber;


    public Emulator() throws IOException {
        configManager = ConfigManager.getInstance();

        // XXX: using deprecated class `ProjectTopicName` due to strange compatibility issue
        topicName = ProjectTopicName.of(configManager.getProject(), configManager.getTopic());

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(configManager.getProject(), configManager.getSubscription());

        channel = ManagedChannelBuilder.forTarget(configManager.getEmulatorHost()).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        topicAdmin = createTopicAdmin(credentialsProvider);
        topicAdmin.createTopic(topicName);

        publisher = createPublisher(credentialsProvider);

        subscriptionAdminClient = createSubscriptionAdmin(credentialsProvider);
        subscription = subscriptionAdminClient.createSubscription(
                subscriptionName,
                topicName,
                PushConfig.getDefaultInstance(),
                0);
        subscriber = createSubscriberStub(credentialsProvider);
    }

    public static void main(String[] argv) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        System.out.println("Creating channel and topic...");
        Emulator initializer = new Emulator();
        System.out.println("SUCCESS: channel and topic are successfully created");
        System.out.println("Loading dataset to the topic...");
        initializer.load();
        System.out.println("SUCCESS: all lines are published");
        System.out.println("Cleaning up...");
        initializer.clean();
    }

    public void load() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        try (BufferedReader br = new BufferedReader(configManager.getDatasetReader())) {
            String line;
            int currentLine = 0;
            int max = configManager.getEmulatorMaxLine();
            while ((line = br.readLine()) != null && currentLine <= max) {
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(line)).build();
                publisher.publish(pubsubMessage).get(10, TimeUnit.SECONDS);
                currentLine++;
            }
        }
    }

    public void clean() {
        topicAdmin.deleteTopic(topicName);
        if (subscription != null) {
            subscriptionAdminClient.deleteSubscription(subscription.getName());
        }
        channel.shutdownNow();
    }

    public String getSubscription() {
        return subscription.getName();
    }

    public SubscriberStub getSubscriber() {
        return subscriber;
    }

    private TopicAdminClient createTopicAdmin(CredentialsProvider credentialsProvider) throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build()
        );
    }

    private Publisher createPublisher(CredentialsProvider credentialsProvider) throws IOException {
        return Publisher.newBuilder(topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
    }

    private SubscriptionAdminClient createSubscriptionAdmin(CredentialsProvider credentialsProvider) throws IOException {
        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider)
                .setTransportChannelProvider(channelProvider)
                .build();
        return SubscriptionAdminClient.create(subscriptionAdminSettings);
    }

    private SubscriberStub createSubscriberStub(CredentialsProvider credentialsProvider) throws IOException {
        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
        return GrpcSubscriberStub.create(subscriberStubSettings);
    }
}
