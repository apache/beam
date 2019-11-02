package org.apache.beam.sdk.io.aws2.sqs;

import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

import java.net.URI;

class EmbeddedSqsServer {
  private static SQSRestServer sqsRestServer;
  private static SqsClient client;
  private static String queueUrl;
  private static String ENDPOINT = "http://localhost:9324";
  private static String QUEUE_NAME = "test";

  static void start() {
    sqsRestServer = SQSRestServerBuilder.start();

    client =
        SqsClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_WEST_2)
            .build();

    CreateQueueRequest createQueueRequest =
        CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
    final CreateQueueResponse queue = client.createQueue(createQueueRequest);
    queueUrl = queue.queueUrl();
  }

  static SqsClient getClient() {
    return client;
  }

  static String getQueueUrl() {
    return queueUrl;
  }

  static String getEndpoint() { return ENDPOINT; }

  static void stop() {
    //sqsRestServer.stopAndWait();
  }
}
