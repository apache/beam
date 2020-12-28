package org.apache.beam.examples.complete.kafkatopubsub;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubJsonClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Set up pubsub container.
 */
public class SetupPubsubContainer {

  private static final String PROJECT_ID = "try-kafka-pubsub";
  private static final String TOPIC_NAME = "listen-to-kafka";
  private static final TopicPath TOPIC_PATH = PubsubClient
      .topicPathFromName(PROJECT_ID, TOPIC_NAME);


  SetupPubsubContainer(TestPipeline pipeline) throws Exception {
    PubsubOptions options = pipeline.getOptions().as(PubsubOptions.class);

    String pubsubUrl = setupPubsubContainer();
    options.setPubsubRootUrl("http://" + pubsubUrl);

    PubsubClient pubsubClient = PubsubJsonClient.FACTORY.newClient(null, null, options);
    pubsubClient.createTopic(TOPIC_PATH);
    System.out.println(pubsubClient.listTopics(PubsubClient.projectPathFromId(PROJECT_ID)));
  }

  private String setupPubsubContainer() {
    PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
        DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators")
    );
    emulator.start();
    return emulator.getEmulatorEndpoint();
  }

  public static String getTopicPath() {
    return TOPIC_PATH.getPath();
  }

}
