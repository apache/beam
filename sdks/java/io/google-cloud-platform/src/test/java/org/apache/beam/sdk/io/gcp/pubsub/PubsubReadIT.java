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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for PubsubIO. */
@RunWith(JUnit4.class)
public class PubsubReadIT {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubReadIT.class);

  @Rule public transient TestPubsubSignal signal = TestPubsubSignal.create();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();



  @Test
  public void testReadPubsubMessageId() throws Exception {
    // The pipeline will never terminate on its own
    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);

    SubscriberOptions subscriberOpitons =
        SubscriberOptions.newBuilder()
            .setSubscriptionPath(SubscriptionPath.parse("projects/927334603519/locations/us-central1-a/subscriptions/boyuanz-pubsublite-sub-test"))
            .build();

    pipeline
        .apply("Create elements", PubsubLiteIO.read(subscriberOpitons))
        .apply("Convert messages", MapElements.into(TypeDescriptors.strings()).via(
            (SequencedMessage sequencedMessage) -> {
              String data = sequencedMessage.getMessage().getData().toStringUtf8();
              LOG.info("Received: " + data);
              return data;
            }
        ))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5L))))
        .apply(WithKeys.of("bobcat"))
        .apply(GroupByKey.create())
        .apply(FlatMapElements.into(TypeDescriptors.nulls()).via(
            (KV<String, Iterable<String>> pair) -> {
              LOG.info("Key: " + pair.getKey());
              pair.getValue().forEach(message -> LOG.info("Flattened elements: " + message));
              return Collections.EMPTY_LIST;
            }
        ));

    pipeline.run().waitUntilFinish();
  }

  private static class NonEmptyMessageIdCheck
      implements SerializableFunction<Set<PubsubMessage>, Boolean> {
    @Override
    public Boolean apply(Set<PubsubMessage> input) {
      for (PubsubMessage message : input) {
        if (Strings.isNullOrEmpty(message.getMessageId())) {
          return false;
        }
      }
      return true;
    }
  }
}
