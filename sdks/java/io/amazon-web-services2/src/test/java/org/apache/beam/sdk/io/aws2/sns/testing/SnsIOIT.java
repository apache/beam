/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.aws2.sns.testing;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.ITEnvironment;
import org.apache.beam.sdk.io.aws2.sns.SnsIO;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO;
import org.apache.beam.sdk.io.aws2.sqs.SqsMessage;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.common.TestRow.DeterministicallyConstructTestRowFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Integration test to write to SNS.
 *
 * <p>Records are then read via SQS for validation.
 *
 * <p>By default this runs against Localstack, but you can use {@link SnsIOIT.ITOptions} to
 * configure tests to run against AWS SNS / SQS.
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services2:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws2.sns.testing.SnsIOIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class SnsIOIT {
  public interface ITOptions extends ITEnvironment.ITOptions {}

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ClassRule
  public static ITEnvironment<ITOptions> env =
      new ITEnvironment<>(new Service[] {SQS, SNS}, ITOptions.class, "SQS_PROVIDER=elasticmq");

  @Rule public Timeout globalTimeout = Timeout.seconds(600);

  @Rule public TestPipeline pipelineWrite = env.createTestPipeline();
  @Rule public TestPipeline pipelineRead = env.createTestPipeline();
  @Rule public AwsResources resources = new AwsResources();

  @Test
  public void testWriteThenRead() {
    ITOptions opts = env.options();
    int rows = opts.getNumberOfRows();

    // Write test dataset to SNS
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(rows))
        .apply("Prepare TestRows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply(
            "Write to SNS",
            SnsIO.<TestRow>write()
                .withTopicArn(resources.snsTopic)
                .withPublishRequestBuilder(r -> PublishRequest.builder().message(r.name())));

    // Read test dataset from SQS.
    PCollection<String> output =
        pipelineRead
            .apply(
                "Read from SQS",
                SqsIO.read().withQueueUrl(resources.sqsQueue).withMaxNumRecords(rows))
            .apply("Extract message", MapElements.into(strings()).via(SnsIOIT::extractMessage));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo((long) rows);

    PAssert.that(output.apply(Combine.globally(new HashingFn()).withoutDefaults()))
        .containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineWrite.run();
    pipelineRead.run();
  }

  private static String extractMessage(SqsMessage msg) {
    try {
      return MAPPER.readTree(msg.getBody()).get("Message").asText();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static class AwsResources extends ExternalResource implements Serializable {
    private transient SqsClient sqs = env.buildClient(SqsClient.builder());
    private transient SnsClient sns = env.buildClient(SnsClient.builder());

    private String sqsQueue;
    private String snsTopic;
    private String sns2Sqs;

    @Override
    protected void before() throws Throwable {
      snsTopic = sns.createTopic(b -> b.name("beam-snsio-it")).topicArn();
      // add SQS subscription so we can read the messages again
      sqsQueue = sqs.createQueue(b -> b.queueName("beam-snsio-it")).queueUrl();
      sns2Sqs =
          sns.subscribe(b -> b.topicArn(snsTopic).endpoint(sqsQueue).protocol("sqs"))
              .subscriptionArn();
    }

    @Override
    protected void after() {
      try {
        executeWithRetry(() -> sns.unsubscribe(b -> b.subscriptionArn(sns2Sqs)));
        executeWithRetry(() -> sns.deleteTopic(b -> b.topicArn(snsTopic)));
        executeWithRetry(() -> sqs.deleteQueue(b -> b.queueUrl(sqsQueue)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        sns.close();
        sqs.close();
      }
    }
  }
}
