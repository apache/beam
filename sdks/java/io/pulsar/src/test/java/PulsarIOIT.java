import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class PulsarIOIT {

    private static final String PULSAR_URL = "pulsar://localhost:6650";
    private static final String PULSAR_SERVICE = "pulsar_s1";
    private static final String PULSAR_ADMIN_PROTOCOL = "http";
    private static final int PULSAR_ADMIN_PORT = 8080;
    private static final int PULSAR_DATA_PORT = 6650;


    private static PulsarContainer pulsarContainer;

    @BeforeClass
    public static void setup() throws PulsarClientException {
        setupPulsarContainer();

    }

    @AfterClass
    public static void afterClass() {
        if(pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testPulsarClient() throws PulsarClientException{
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .build();
    }

    private static void setupPulsarContainer() {
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar"));
        pulsarContainer.withExposedPorts(6650, 8080);
        pulsarContainer.withCommand("bin/pulsar standalone");
        pulsarContainer.waitingFor(Wait.forLogMessage(".*Function worker service started.*", 1));
        pulsarContainer.start();
    }
}
