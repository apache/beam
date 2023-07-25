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
package org.apache.beam.sdk.io.aws.sqs;

import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.io.Serializable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws.ITEnvironment;
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
            .apply("Extract body", MapElements.into(strings()).via(Message::getBody));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo((long) rows);

    PAssert.that(output.apply(Combine.globally(new HashingFn()).withoutDefaults()))
        .containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineWrite.run();
    pipelineRead.run();
  }

  private static class SqsQueue extends ExternalResource implements Serializable {
    private transient AmazonSQS client = env.buildClient(AmazonSQSClientBuilder.standard());
    private String url;

    SendMessageRequest messageRequest(TestRow r) {
      return new SendMessageRequest(url, r.name());
    }

    @Override
    protected void before() {
      url = client.createQueue("beam-sqsio-it").getQueueUrl();
    }

    @Override
    protected void after() {
      client.deleteQueue(url);
      client.shutdown();
    }
  }
}
