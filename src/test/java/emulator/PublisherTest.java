package emulator;

import org.junit.Before;
import org.junit.Test;
import pubsub.EmulatorPublisher;

import java.io.IOException;

public class PublisherTest {
    ConfigManager configManager;

    @Before
    public void setUp() throws IOException {
        configManager = ConfigManager.getInstance();
    }

    @Test
    public void testMessagePublish() throws IOException {
        EmulatorPublisher.createTopic();
        EmulatorPublisher.createSubscription();
        EmulatorPublisher.publishMessages();
        EmulatorPublisher.deleteSubscription();
        EmulatorPublisher.deleteTopic();
    }
}
