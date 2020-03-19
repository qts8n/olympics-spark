package pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import emulator.ConfigManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class EmulatorPublisher {
    public static ManagedChannel getChannel() throws IOException {
        return ManagedChannelBuilder
                .forTarget(ConfigManager.getInstance().getEmulatorHost())
                .usePlaintext()
                .build();
    }

    public static TransportChannelProvider createProvider(ManagedChannel channel) {
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }

    private static TopicAdminClient createTopicAdminClient(ManagedChannel channel) throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(createProvider(channel))
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build());
    }

    private static SubscriptionAdminClient createSubscriptionAdminClient(ManagedChannel channel) throws IOException {
        SubscriptionAdminSettings subscriptionAdminSettings= SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(createProvider(channel))
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();
        return SubscriptionAdminClient.create(subscriptionAdminSettings);
    }

    public static ProjectTopicName getTopic() throws IOException {
        ConfigManager configManager = ConfigManager.getInstance();
        return ProjectTopicName.of(configManager.getProject(), configManager.getTopic());
    }

    public static void createTopic() throws IOException {
        ManagedChannel channel = getChannel();
        try (TopicAdminClient client = createTopicAdminClient(channel)) {
            client.createTopic(getTopic());
        } finally {
            channel.shutdownNow();
        }
    }

    public static void deleteTopic() throws IOException {
        ManagedChannel channel = getChannel();
        try (TopicAdminClient client = createTopicAdminClient(channel)) {
            client.deleteTopic(getTopic());
        } finally {
            channel.shutdownNow();
        }
    }

    public static ProjectSubscriptionName getSubscription() throws IOException {
        ConfigManager configManager = ConfigManager.getInstance();
        return ProjectSubscriptionName.of(configManager.getProject(), configManager.getSubscription());
    }

    public static void createSubscription() throws IOException {
        ManagedChannel channel = getChannel();
        try (SubscriptionAdminClient client = createSubscriptionAdminClient(channel)) {
            client.createSubscription(
                    getSubscription(),
                    getTopic(),
                    PushConfig.getDefaultInstance(),
                    0);
        } finally {
            channel.shutdownNow();
        }
    }

    public static void deleteSubscription() throws IOException {
        ManagedChannel channel = getChannel();
        try (SubscriptionAdminClient client = createSubscriptionAdminClient(channel)) {
            client.deleteSubscription(getSubscription());
        } finally {
            channel.shutdownNow();
        }
    }

    public static void publishMessages() throws IOException {
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
        ManagedChannel channel = getChannel();
        try {
            TransportChannelProvider channelProvider = createProvider(channel);
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(getTopic())
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();

            List<String> messages = new ArrayList<>();
            ConfigManager configManager = ConfigManager.getInstance();
            try (BufferedReader br = new BufferedReader(configManager.getDatasetReader())) {
                String line;
                int currentLine = 0;
                int max = configManager.getEmulatorMaxLine();
                while ((line = br.readLine()) != null && currentLine <= max) {
                    messages.add(line);
                    currentLine++;
                }
            }

            // schedule publishing one message at a time : messages get automatically batched
            for (String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            }
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = new ArrayList<>();
            try {
                messageIds = ApiFutures.allAsList(messageIdFutures).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            for (String messageId : messageIds) {
                System.out.println("published with message ID: " + messageId);
            }

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                try {
                    publisher.awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            channel.shutdownNow();
        }
    }
}
