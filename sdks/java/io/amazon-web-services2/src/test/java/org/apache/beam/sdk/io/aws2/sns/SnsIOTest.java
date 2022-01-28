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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.Duration.millis;
import static org.joda.time.Duration.standardSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.InternalErrorException;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Tests to verify writes to Sns. */
@RunWith(MockitoJUnitRunner.class)
public class SnsIOTest implements Serializable {

  private static final String topicArn = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public TestPipeline p = TestPipeline.create();
  @Mock public SnsClient sns;

  @Rule
  public final transient ExpectedLogs snsWriterFnLogs =
      ExpectedLogs.none(SnsIO.Write.SnsWriterFn.class);

  @Test
  public void testFailOnTopicValidation() {
    when(sns.getTopicAttributes(any(Consumer.class)))
        .thenThrow(InvalidParameterException.builder().message("Topic does not exist").build());

    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withTopicArn(topicArn)
            .withPublishRequestBuilder(msg -> requestBuilder(msg, "ignore"))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns));

    assertThatThrownBy(() -> snsWrite.expand(mock(PCollection.class)))
        .hasMessage("Topic arn " + topicArn + " does not exist");
  }

  @Test
  public void testSkipTopicValidation() {
    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns));

    snsWrite.expand(mock(PCollection.class));
    verify(sns, times(0)).getTopicAttributes(any(Consumer.class));
  }

  @Test
  public void testWriteWithTopicArn() {
    List<String> input = ImmutableList.of("message1", "message2");

    when(sns.publish(any(PublishRequest.class)))
        .thenReturn(PublishResponse.builder().messageId("id").build());

    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withTopicArn(topicArn)
            .withPublishRequestBuilder(msg -> requestBuilder(msg, "ignore"))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns));

    PCollection<PublishResponse> results = p.apply(Create.of(input)).apply(snsWrite);
    PAssert.that(results.apply(Count.globally())).containsInAnyOrder(2L);
    p.run();

    verify(sns).getTopicAttributes(any(Consumer.class));
    for (String msg : input) {
      verify(sns).publish(requestBuilder(msg, topicArn).build());
    }
  }

  @Test
  public void testWriteWithoutTopicArn() {
    List<String> input = ImmutableList.of("message1", "message2");

    when(sns.publish(any(PublishRequest.class)))
        .thenReturn(PublishResponse.builder().messageId("id").build());

    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns));

    PCollection<PublishResponse> results = p.apply(Create.of(input)).apply(snsWrite);
    PAssert.that(results.apply(Count.globally())).containsInAnyOrder(2L);
    p.run();

    verify(sns, times(0)).getTopicAttributes(any(Consumer.class));
    for (String msg : input) {
      verify(sns).publish(requestBuilder(msg, topicArn).build());
    }
  }

  @Test
  public void testWriteWithRetries() {
    List<String> input = ImmutableList.of("message1", "message2");

    when(sns.publish(any(PublishRequest.class)))
        .thenThrow(InternalErrorException.builder().message("Service unavailable").build());

    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns))
            .withRetryConfiguration(
                SnsIO.RetryConfiguration.create(4, standardSeconds(10), millis(1)));

    p.apply(Create.of(input)).apply(snsWrite);

    assertThatThrownBy(() -> p.run())
        .isInstanceOf(PipelineExecutionException.class)
        .hasCauseInstanceOf(IOException.class)
        .hasMessageContaining("Error writing to SNS after 4 attempt(s). No more attempts allowed");

    // check 3 retries were initiated by inspecting the log before passing on the exception
    snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 1));
    snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 2));
    snsWriterFnLogs.verifyWarn(String.format(SnsIO.Write.SnsWriterFn.RETRY_ATTEMPT_LOG, 3));
  }

  @Test
  public void testWriteWithCustomCoder() {
    List<String> input = ImmutableList.of("message1");

    when(sns.publish(any(PublishRequest.class)))
        .thenReturn(PublishResponse.builder().messageId("id").build());

    // Mockito mocks cause NotSerializableException even with withSettings().serializable()
    final CountingFn<PublishResponse> countingFn = new CountingFn<>();

    SnsIO.Write<String> snsWrite =
        SnsIO.<String>write()
            .withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn))
            .withSnsClientProvider(StaticSnsClientProvider.of(sns))
            .withCoder(DelegateCoder.of(defaultPublishResponse(), countingFn, x -> x));

    PCollection<PublishResponse> results = p.apply(Create.of(input)).apply(snsWrite);
    PAssert.that(results.apply(Count.globally())).containsInAnyOrder(1L);
    p.run();

    assertThat(countingFn.count).isGreaterThan(0);
    for (String msg : input) {
      verify(sns).publish(requestBuilder(msg, topicArn).build());
    }
  }

  private static class CountingFn<T> implements CodingFunction<T, T> {
    int count;

    @Override
    public T apply(T input) throws Exception {
      count++;
      return input;
    }
  }

  private static PublishRequest.Builder requestBuilder(String msg, String topic) {
    return PublishRequest.builder().message(msg).topicArn(topic);
  }
}
