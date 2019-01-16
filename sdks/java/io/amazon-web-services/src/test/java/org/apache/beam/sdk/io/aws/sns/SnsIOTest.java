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

import static org.junit.Assert.fail;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests to verify writes to Sns. */
@RunWith(JUnit4.class)
public class SnsIOTest implements Serializable {

  private static final String topicName = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public TestPipeline p = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(SnsIO.class);

  private static PublishRequest createSampleMessage(String message) {
    return new PublishRequest().withTopicArn(topicName).withMessage(message);
  }

  private static class Provider implements AwsClientsProvider {

    private static AmazonSNS publisher;

    public Provider(AmazonSNS pub) {
      publisher = pub;
    }

    @Override
    public AmazonCloudWatch getCloudWatchClient() {
      return Mockito.mock(AmazonCloudWatch.class);
    }

    @Override
    public AmazonSNS createSnsPublisher() {
      return publisher;
    }
  }

  @Test
  public void testDataWritesToSNS() {
    final PublishRequest request1 = createSampleMessage("my_first_message");
    final PublishRequest request2 = createSampleMessage("my_second_message");

    final TupleTag<PublishResult> results = new TupleTag<>();

    final PCollectionTuple snsWrites =
        p.apply(Create.of(request1, request2))
            .apply(
                SnsIO.write()
                    .withTopicName(topicName)
                    .withRetryConfiguration(
                        SnsIO.RetryConfiguration.create(
                            5, org.joda.time.Duration.standardMinutes(1)))
                    .withAWSClientsProvider(new Provider(new AmazonSNSMockSuccess()))
                    .withResultOutputTag(results));

    final PCollection<Long> publishedResultsSize = snsWrites.get(results).apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(ImmutableList.of(2L));
    p.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to SNS");
    final PublishRequest request1 = createSampleMessage("my message that will not be published");
    final TupleTag<PublishResult> results = new TupleTag<>();
    p.apply(Create.of(request1))
        .apply(
            SnsIO.write()
                .withTopicName(topicName)
                .withRetryConfiguration(
                    SnsIO.RetryConfiguration.create(4, org.joda.time.Duration.standardSeconds(10)))
                .withAWSClientsProvider(new Provider(new AmazonSNSMockErrors()))
                .withResultOutputTag(results));

    try {
      p.run();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to SNS.");
  }
}
