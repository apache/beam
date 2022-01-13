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
package org.apache.beam.sdk.io.aws.sns;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws.ITEnvironment;
import org.apache.beam.sdk.io.aws.sqs.SqsIO;
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
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

@RunWith(JUnit4.class)
public class SnsIOIT {
  public interface ITOptions extends ITEnvironment.ITOptions {}

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeDescriptor<PublishRequest> publishRequests =
      TypeDescriptor.of(PublishRequest.class);

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
        .apply("SNS request", MapElements.into(publishRequests).via(resources::publishRequest))
        .apply(
            "Write to SNS",
            SnsIO.write()
                .withTopicName(resources.snsTopic)
                .withResultOutputTag(new TupleTag<>())
                .withAWSClientsProvider(
                    opts.getAwsCredentialsProvider().getCredentials().getAWSAccessKeyId(),
                    opts.getAwsCredentialsProvider().getCredentials().getAWSSecretKey(),
                    Regions.fromName(opts.getAwsRegion()),
                    opts.getAwsServiceEndpoint()));

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

  private static String extractMessage(Message msg) {
    try {
      return MAPPER.readTree(msg.getBody()).get("Message").asText();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static class AwsResources extends ExternalResource implements Serializable {
    private transient AmazonSQS sqs = env.buildClient(AmazonSQSClientBuilder.standard());
    private transient AmazonSNS sns = env.buildClient(AmazonSNSClientBuilder.standard());

    private String sqsQueue;
    private String snsTopic;
    private String sns2Sqs;

    PublishRequest publishRequest(TestRow r) {
      return new PublishRequest(snsTopic, r.name());
    }

    @Override
    protected void before() throws Throwable {
      snsTopic = sns.createTopic("beam-snsio-it").getTopicArn();
      // add SQS subscription so we can read the messages again
      sqsQueue = sqs.createQueue("beam-snsio-it").getQueueUrl();
      sns2Sqs = sns.subscribe(snsTopic, "sqs", sqsQueue).getSubscriptionArn();
    }

    @Override
    protected void after() {
      try {
        executeWithRetry(() -> sns.unsubscribe(sns2Sqs));
        executeWithRetry(() -> sns.deleteTopic(snsTopic));
        executeWithRetry(() -> sqs.deleteQueue(sqsQueue));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        sns.shutdown();
        sqs.shutdown();
      }
    }
  }
}
