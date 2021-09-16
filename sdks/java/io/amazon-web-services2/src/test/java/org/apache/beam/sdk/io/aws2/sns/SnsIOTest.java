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
package org.apache.beam.sdk.io.aws2.sns;

import static org.junit.Assert.fail;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Tests to verify writes to Sns. */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PublishResponse.class, GetTopicAttributesResponse.class})
public class SnsIOTest implements Serializable {

  private static final String topicArn = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public TestPipeline p = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(SnsIO.class);

  private static PublishRequest createSampleMessage(String message) {
    return PublishRequest.builder().topicArn(topicArn).message(message).build();
  }

  @Test
  public void testDataWritesToSNS() {
    ImmutableList<String> input = ImmutableList.of("message1", "message2");

    final PCollection<PublishResponse> results =
        p.apply(Create.of(input))
            .apply(
                SnsIO.<String>write()
                    .withPublishRequestFn(SnsIOTest::createSampleMessage)
                    .withTopicArn(topicArn)
                    .withRetryConfiguration(
                        SnsIO.RetryConfiguration.create(
                            5, org.joda.time.Duration.standardMinutes(1)))
                    .withSnsClientProvider(SnsClientMockSuccess::new));

    final PCollection<Long> publishedResultsSize = results.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(ImmutableList.of(2L));
    p.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to SNS");

    ImmutableList<String> input = ImmutableList.of("message1", "message2");

    p.apply(Create.of(input))
        .apply(
            SnsIO.<String>write()
                .withPublishRequestFn(SnsIOTest::createSampleMessage)
                .withTopicArn(topicArn)
                .withRetryConfiguration(
                    SnsIO.RetryConfiguration.create(4, org.joda.time.Duration.standardSeconds(10)))
                .withSnsClientProvider(SnsClientMockErrors::new));

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
