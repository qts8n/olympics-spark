package emulator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


/**
 * Config properties manager singleton.
 */
public final class ConfigManager {
    private static ConfigManager instance;
    private Properties props;

    private ConfigManager() throws FileNotFoundException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            props = new Properties();
            props.load(in);
        } catch (IOException e) {
            throw new FileNotFoundException("Could no find properties file");
        }
    }

    public static ConfigManager getInstance() throws FileNotFoundException {
        if (instance == null) {
            instance = new ConfigManager();
        }
        return instance;
    }

    public String getAppName() {
        return props.getProperty("app-name", "App");
    }

    public String getProject() {
        return props.getProperty("project");
    }

    public String getTopic() {
        return props.getProperty("topic");
    }

    public String getSubscription() {
        return props.getProperty("subscription");
    }

    public String getEmulatorHost() {
        return props.getProperty("emulator-host", "localhost:8085");
    }

    public int getEmulatorMaxLine() throws NumberFormatException {
        return Integer.parseInt(props.getProperty("emulator-maxline", "50"));
    }

    public Reader getDatasetReader() {
        InputStream initialStream = getClass().getClassLoader().getResourceAsStream(props.getProperty("dataset-filename"));
        assert initialStream != null;
        return new InputStreamReader(initialStream, StandardCharsets.UTF_8);
    }
}
