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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.sns.model.PublishRequest;

@RunWith(JUnit4.class)
public class SnsIOWriteTest implements Serializable {
  private static final String TOPIC = "test";
  private static final int FAILURE_STATUS_CODE = 400;
  private static final int SUCCESS_STATUS_CODE = 200;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void shouldReturnResponseOnPublishSuccess() {
    String testMessage1 = "test1";
    String testMessage2 = "test2";
    String testMessage3 = "test3";

    PCollection<SnsResponse<String>> result =
        pipeline
            .apply(
                Create.of(testMessage1, testMessage2, testMessage3).withCoder(StringUtf8Coder.of()))
            .apply(
                SnsIO.<String>writeAsync()
                    .withCoder(StringUtf8Coder.of())
                    .withPublishRequestFn(createPublishRequestFn())
                    .withSnsClientProvider(
                        () -> MockSnsAsyncClient.withStatusCode(SUCCESS_STATUS_CODE)));

    PAssert.that(result)
        .satisfies(
            (responses) -> {
              ImmutableSet<String> messagesInResponse =
                  StreamSupport.stream(responses.spliterator(), false)
                      .filter(response -> response.statusCode().getAsInt() == SUCCESS_STATUS_CODE)
                      .map(SnsResponse::element)
                      .collect(ImmutableSet.toImmutableSet());

              Set<String> originalMessages =
                  Sets.newHashSet(testMessage1, testMessage2, testMessage3);
              Sets.SetView<String> difference =
                  Sets.difference(messagesInResponse, originalMessages);

              assertEquals(3, messagesInResponse.size());
              assertEquals(0, difference.size());
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void shouldReturnResponseOnPublishFailure() {
    String testMessage1 = "test1";
    String testMessage2 = "test2";

    PCollection<SnsResponse<String>> result =
        pipeline
            .apply(Create.of(testMessage1, testMessage2).withCoder(StringUtf8Coder.of()))
            .apply(
                SnsIO.<String>writeAsync()
                    .withCoder(StringUtf8Coder.of())
                    .withPublishRequestFn(createPublishRequestFn())
                    .withSnsClientProvider(
                        () -> MockSnsAsyncClient.withStatusCode(FAILURE_STATUS_CODE)));

    PAssert.that(result)
        .satisfies(
            (responses) -> {
              ImmutableSet<String> messagesInResponse =
                  StreamSupport.stream(responses.spliterator(), false)
                      .filter(response -> response.statusCode().getAsInt() != SUCCESS_STATUS_CODE)
                      .map(SnsResponse::element)
                      .collect(ImmutableSet.toImmutableSet());

              Set<String> originalMessages = Sets.newHashSet(testMessage1, testMessage2);
              Sets.SetView<String> difference =
                  Sets.difference(messagesInResponse, originalMessages);

              assertEquals(2, messagesInResponse.size());
              assertEquals(0, difference.size());
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  @SuppressWarnings("MissingFail")
  public void shouldThrowIfThrowErrorOptionSet() {
    String testMessage1 = "test1";

    pipeline
        .apply(Create.of(testMessage1))
        .apply(
            SnsIO.<String>writeAsync()
                .withCoder(StringUtf8Coder.of())
                .withPublishRequestFn(createPublishRequestFn())
                .withSnsClientProvider(
                    () -> MockSnsAsyncClient.withStatusCode(FAILURE_STATUS_CODE)));
    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      assertThrows(IOException.class, () -> e.getCause().getClass());
    }
  }

  @Test
  @SuppressWarnings("MissingFail")
  public void shouldThrowIfThrowErrorOptionSetOnInternalException() {
    String testMessage1 = "test1";

    pipeline
        .apply(Create.of(testMessage1))
        .apply(
            SnsIO.<String>writeAsync()
                .withCoder(StringUtf8Coder.of())
                .withPublishRequestFn(createPublishRequestFn())
                .withSnsClientProvider(MockSnsAsyncExceptionClient::create));
    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      assertThrows(IOException.class, () -> e.getCause().getClass());
    }
  }

  private SerializableFunction<String, PublishRequest> createPublishRequestFn() {
    return (input) -> PublishRequest.builder().topicArn(TOPIC).message(input).build();
  }
}
