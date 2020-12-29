package org.apache.beam.examples.complete.kafkatopubsub;

import static org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform.readFromKafka;

import com.google.auth.Credentials;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.beam.examples.complete.kafkatopubsub.utils.RunKafkaContainer;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubJsonClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * E2E test for {@link KafkaToPubsub} pipeline.
 */
public class KafkaToPubsubE2ETest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);
    @Rule
    public transient TestPubsubSignal signal = TestPubsubSignal.fromOptions(OPTIONS);

    private static final String PUBSUB_EMULATOR_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators";
    private static final String PUBSUB_MESSAGE = "test pubsub message";
    private static final String PROJECT_ID = "try-kafka-pubsub";
    private static final String TOPIC_NAME = "listen-to-kafka";
    private static final PubsubClient.TopicPath TOPIC_PATH = PubsubClient
            .topicPathFromName(PROJECT_ID, TOPIC_NAME);
    private static final PipelineOptions OPTIONS = TestPipeline.testingPipelineOptions();

    @BeforeClass
    public static void beforeClass() throws Exception {
        Credentials credentials = NoopCredentialFactory.fromOptions(OPTIONS).getCredential();
        OPTIONS.as(GcpOptions.class).setGcpCredential(credentials);
        OPTIONS.as(GcpOptions.class).setProject(PROJECT_ID);
        setupPubsubContainer(OPTIONS.as(PubsubOptions.class));
        createPubsubTopicForTest(OPTIONS.as(PubsubOptions.class));
    }

    @Test
    public void testKafkaToPubsubE2E() throws IOException {
        pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

        RunKafkaContainer rkc = new RunKafkaContainer(PUBSUB_MESSAGE);
        String bootstrapServer = rkc.getBootstrapServer();
        String[] kafkaTopicsList = new String[]{rkc.getTopicName()};

        String pubsubTopicPath = TOPIC_PATH.getPath();

        Map<String, Object> kafkaConfig = new HashMap<>();
        Map<String, String> sslConfig = new HashMap<>();

        PCollection<KV<String, String>> readStrings = pipeline
                .apply("readFromKafka",
                        readFromKafka(bootstrapServer, Arrays.asList(kafkaTopicsList), kafkaConfig, sslConfig));

        PCollection<String> readFromPubsub = readStrings.apply(Values.create())
                .apply("writeToPubSub", PubsubIO.writeStrings().to(pubsubTopicPath)).getPipeline()
                .apply("readFromPubsub",
                        PubsubIO.readStrings().fromTopic(pubsubTopicPath));

        readFromPubsub.apply("waitForTestMessage", signal.signalSuccessWhen(
                readFromPubsub.getCoder(),
                input -> {
                    if (input == null) return false;
                    return input.stream().anyMatch(message -> Objects.equals(message, PUBSUB_MESSAGE));
                })
        );

        Supplier<Void> start = signal.waitForStart(Duration.standardSeconds(10));
        pipeline.apply(signal.signalStart());
        PipelineResult job = pipeline.run();
        start.get();
        signal.waitForSuccess(Duration.standardMinutes(2));
        try {
            job.cancel();
        } catch (IOException | UnsupportedOperationException e) {
            throw new AssertionError("Could not stop pipeline.", e);
        }
    }

    private static void setupPubsubContainer(PubsubOptions options) {
        PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_EMULATOR_IMAGE));
        emulator.start();
        String pubsubUrl = emulator.getEmulatorEndpoint();
        options.setPubsubRootUrl("http://" + pubsubUrl);
    }

    private static void createPubsubTopicForTest(PubsubOptions options) {
        try {
            PubsubClient pubsubClient = PubsubJsonClient.FACTORY.newClient(null, null, options);
            pubsubClient.createTopic(TOPIC_PATH);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
