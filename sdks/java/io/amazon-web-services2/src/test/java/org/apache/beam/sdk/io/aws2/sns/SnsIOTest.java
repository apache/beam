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

import static org.apache.beam.sdk.io.aws2.sns.PublishResponseCoders.defaultPublishResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.Duration.millis;
import static org.joda.time.Duration.standardSeconds;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.DelegateCoder.CodingFunction;
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
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Tests to verify writes to Sns. */
@RunWith(JUnit4.class)
public class SnsIOTest implements Serializable {

  private static final String topicArn = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public TestPipeline p = TestPipeline.create();

  @Rule
  public final transient ExpectedLogs snsWriterFnLogs =
      ExpectedLogs.none(SnsIO.Write.SnsWriterFn.class);

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
                    .withSnsClientProvider(SnsClientMockSuccess::new));

    final PCollection<Long> publishedResultsSize = results.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(ImmutableList.of(2L));
    p.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expect(IOException.class);
    thrown.expectMessage("Error writing to SNS");
    thrown.expectMessage("No more attempts allowed");

    ImmutableList<String> input = ImmutableList.of("message1", "message2");

    p.apply(Create.of(input))
        .apply(
            SnsIO.<String>write()
                .withPublishRequestFn(SnsIOTest::createSampleMessage)
                .withTopicArn(topicArn)
                .withRetryConfiguration(
                    SnsIO.RetryConfiguration.create(4, standardSeconds(10), millis(1)))
                .withSnsClientProvider(SnsClientMockErrors::new));

    try {
      p.run();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 1));
      snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 2));
      snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
  }

  @Test
  public void testCustomCoder() throws Exception {
    ImmutableList<String> input = ImmutableList.of("message1");

    // Mockito mocks cause NotSerializableException even with withSettings().serializable()
    final CountingFn<PublishResponse> countingFn = new CountingFn<>();
    final Coder<PublishResponse> coder =
        DelegateCoder.of(defaultPublishResponse(), countingFn, x -> x);

    final PCollection<PublishResponse> results =
        p.apply(Create.of(input))
            .apply(
                SnsIO.<String>write()
                    .withPublishRequestFn(SnsIOTest::createSampleMessage)
                    .withTopicArn(topicArn)
                    .withSnsClientProvider(SnsClientMockSuccess::new)
                    .withCoder(coder));

    final PCollection<Long> publishedResultsSize = results.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(ImmutableList.of(1L));
    p.run().waitUntilFinish();

    assertThat(countingFn.count).isGreaterThan(0);
  }

  private static class CountingFn<T> implements CodingFunction<T, T> {
    int count;

    @Override
    public T apply(T input) throws Exception {
      count++;
      return input;
    }
  }
}
