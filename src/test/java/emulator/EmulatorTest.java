package emulator;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * Pub/Sub local emulator test.
 *
 * Tests push/pull `Emulator` methods.
 */
public class EmulatorTest {
    ConfigManager configManager;

    private Emulator emulator;

    @Before
    public void setUp() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        configManager = ConfigManager.getInstance();

        emulator = new Emulator();
        emulator.load();
    }

    @After
    public void tearDown() {
        emulator.clean();
    }

    @Test
    public void testMessagePull() throws IOException {
        PullRequest pullRequest = PullRequest.newBuilder()
                .setReturnImmediately(false)
                .setMaxMessages(1)
                .setSubscription(emulator.getSubscription())
                .build();
        PullResponse pullResponse = emulator.getSubscriber().pullCallable().call(pullRequest);

        ReceivedMessage message = pullResponse.getReceivedMessages(0);
        Assert.assertNotNull(message);

        PubsubMessage pubsubMessage = message.getMessage();
        Assert.assertNotNull(pubsubMessage);

        ByteString messageData = pubsubMessage.getData();
        Assert.assertNotNull(messageData);

        String messageString = pubsubMessage.getData().toStringUtf8();
        Assert.assertNotNull(messageString);

        try (BufferedReader datasetReader = new BufferedReader(configManager.getDatasetReader())) {
            String line = datasetReader.readLine();
            Assert.assertNotNull(line);
            Assert.assertEquals(line, messageString);
        }
    }
}
