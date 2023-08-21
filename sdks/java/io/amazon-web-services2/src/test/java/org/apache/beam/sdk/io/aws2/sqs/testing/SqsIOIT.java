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
package org.apache.beam.sdk.io.aws2.sqs.testing;

import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import java.io.Serializable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.ITEnvironment;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Integration test to write to and read from SQS.
 *
 * <p>By default this runs against Localstack, but you can use {@link SqsIOIT.SqsITOptions} to
 * configure tests to run against AWS SQS.
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services2:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws2.sqs.testing.SqsIOIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class SqsIOIT {
  public interface SqsITOptions extends ITEnvironment.ITOptions {}

  private static final TypeDescriptor<SendMessageRequest> requestType =
      TypeDescriptor.of(SendMessageRequest.class);

  @ClassRule
  public static ITEnvironment<SqsITOptions> env =
      new ITEnvironment<>(SQS, SqsITOptions.class, "SQS_PROVIDER=elasticmq");

  @Rule public Timeout globalTimeout = Timeout.seconds(600);

  @Rule public TestPipeline pipelineWrite = env.createTestPipeline();
  @Rule public TestPipeline pipelineRead = env.createTestPipeline();
  @Rule public SqsQueue sqsQueue = new SqsQueue();

  @Test
  public void testWriteThenRead() {
    int rows = env.options().getNumberOfRows();

    // Write test dataset to SQS.
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(rows))
        .apply("Prepare TestRows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply("Prepare SQS message", MapElements.into(requestType).via(sqsQueue::messageRequest))
        .apply("Write to SQS", SqsIO.write());

    // Read test dataset from SQS.
    PCollection<String> output =
        pipelineRead
            .apply("Read from SQS", SqsIO.read().withQueueUrl(sqsQueue.url).withMaxNumRecords(rows))
            .apply("Extract body", MapElements.into(strings()).via(SqsMessage::getBody));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo((long) rows);

    PAssert.that(output.apply(Combine.globally(new HashingFn()).withoutDefaults()))
        .containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineWrite.run();
    pipelineRead.run();
  }

  @Test
  public void testWriteBatchesThenRead() {
    int rows = env.options().getNumberOfRows();

    // Write test dataset to SQS.
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(rows))
        .apply("Prepare TestRows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply(
            "Write to SQS",
            SqsIO.<TestRow>writeBatches()
                .withEntryMapper((b, row) -> b.messageBody(row.name()))
                .to(sqsQueue.url));

    // Read test dataset from SQS.
    PCollection<String> output =
        pipelineRead
            .apply("Read from SQS", SqsIO.read().withQueueUrl(sqsQueue.url).withMaxNumRecords(rows))
            .apply("Extract body", MapElements.into(strings()).via(SqsMessage::getBody));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo((long) rows);

    PAssert.that(output.apply(Combine.globally(new HashingFn()).withoutDefaults()))
        .containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineWrite.run();
    pipelineRead.run();
  }

  private static class SqsQueue extends ExternalResource implements Serializable {
    private transient SqsClient client = env.buildClient(SqsClient.builder());
    private String url;

    SendMessageRequest messageRequest(TestRow r) {
      return SendMessageRequest.builder().queueUrl(url).messageBody(r.name()).build();
    }

    @Override
    protected void before() throws Throwable {
      url =
          client.createQueue(b -> b.queueName("beam-sqsio-it-" + randomAlphanumeric(4))).queueUrl();
    }

    @Override
    protected void after() {
      client.deleteQueue(b -> b.queueUrl(url));
      client.close();
    }
  }
}
