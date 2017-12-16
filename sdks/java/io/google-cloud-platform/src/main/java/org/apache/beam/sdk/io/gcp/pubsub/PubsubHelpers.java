package org.apache.beam.sdk.io.gcp.pubsub;

import java.io.IOException;

/** A set of helper functions and classes used by {@link PubsubIO}. */
abstract class PubsubHelpers {

  private static final String RESOURCE_NOT_FOUND_ERROR = "Pubsub %1$s %2$s not found for project"
      + " \"%3$s\". Please create the %1$s %2$s before pipeline"
      + " execution. If the %1$s is created by an earlier stage of the pipeline, this"
      + " validation can be disabled using #withoutValidation.";

  static void validateTopicExists(PubsubClient pubsubClient, String projectId, String topicName)
      throws IOException {
    try {
      boolean topicExists = pubsubClient.topicExistsInProject(projectId, topicName);
      if (!topicExists) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "topic", topicName, projectId));
      }
    } catch (IOException ie) {
      throw new RuntimeException("Was not able to validate options: ", ie);
    } finally {
      pubsubClient.close();
    }
  }

}
