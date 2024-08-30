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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.sns.SnsIO.Write;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Tests to verify writes to Sns. */
@RunWith(MockitoJUnitRunner.class)
public class SnsIOTest implements Serializable {

  private static final String topicArn = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public TestPipeline p = TestPipeline.create();
  @Mock public SnsClient sns;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, SnsClientBuilder.class, sns);
  }

  @Test
  public void testFailOnTopicValidation() {
    PCollection<String> input = mock(PCollection.class);
    when(input.getPipeline()).thenReturn(p);
    when(sns.getTopicAttributes(any(Consumer.class)))
        .thenThrow(InvalidParameterException.builder().message("Topic does not exist").build());

    Write<String> snsWrite =
        SnsIO.<String>write()
            .withTopicArn(topicArn)
            .withPublishRequestBuilder(msg -> requestBuilder(msg, "ignore"));

    assertThatThrownBy(() -> snsWrite.expand(input))
        .hasMessage("Topic arn " + topicArn + " does not exist");
  }

  @Test
  public void testSkipTopicValidation() {
    PCollection<String> input = mock(PCollection.class);
    when(input.getPipeline()).thenReturn(p);
    when(input.apply(any(PTransform.class))).thenReturn(mock(PCollection.class));

    Write<String> snsWrite =
        SnsIO.<String>write().withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn));

    snsWrite.expand(input);
    verify(sns, times(0)).getTopicAttributes(any(Consumer.class));
  }

  @Test
  public void testWriteWithTopicArn() {
    List<String> input = ImmutableList.of("message1", "message2");

    when(sns.publish(any(PublishRequest.class)))
        .thenReturn(PublishResponse.builder().messageId("id").build());

    Write<String> snsWrite =
        SnsIO.<String>write()
            .withTopicArn(topicArn)
            .withPublishRequestBuilder(msg -> requestBuilder(msg, "ignore"));

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

    Write<String> snsWrite =
        SnsIO.<String>write().withPublishRequestBuilder(msg -> requestBuilder(msg, topicArn));

    PCollection<PublishResponse> results = p.apply(Create.of(input)).apply(snsWrite);
    PAssert.that(results.apply(Count.globally())).containsInAnyOrder(2L);
    p.run();

    verify(sns, times(0)).getTopicAttributes(any(Consumer.class));
    for (String msg : input) {
      verify(sns).publish(requestBuilder(msg, topicArn).build());
    }
  }

  private static PublishRequest.Builder requestBuilder(String msg, String topic) {
    return PublishRequest.builder().message(msg).topicArn(topic);
  }
}
