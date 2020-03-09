package emulator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
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
        List<String> messages = emulator.getMessages();
        String receiveMessageText = messages.iterator().next();
        Assert.assertNotNull(receiveMessageText);

        try (BufferedReader datasetReader = new BufferedReader(configManager.getDatasetReader())) {
            String line = datasetReader.readLine();
            Assert.assertNotNull(line);
            Assert.assertEquals(line, receiveMessageText);
        }
    }
}
