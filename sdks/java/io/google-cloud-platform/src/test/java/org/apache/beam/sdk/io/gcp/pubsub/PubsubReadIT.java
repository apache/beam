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

import java.util.Set;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
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
  public void testReadPublicData() throws Exception {
    // The pipeline will never terminate on its own
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    PCollection<String> messages =
        pipeline.apply(
            PubsubIO.readStrings()
                .fromTopic("projects/pubsub-public-data/topics/taxirides-realtime"));

    messages.apply(
        "waitForAnyMessage", signal.signalSuccessWhen(messages.getCoder(), anyMessages -> true));

    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(5));
    pipeline.apply(signal.signalStart());
    PipelineResult job = pipeline.run();
    start.get();

    signal.waitForSuccess(Duration.standardMinutes(5));
    // A runner may not support cancel
    try {
      job.cancel();
    } catch (UnsupportedOperationException exc) {
      // noop
    }
  }

  @Test
  public void testReadPubsubMessageId() throws Exception {
    // The pipeline will never terminate on its own
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    PCollection<PubsubMessage> messages =
        pipeline.apply(
            PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromTopic("projects/pubsub-public-data/topics/taxirides-realtime"));

    messages.apply(
        "isMessageIdNonNull",
        signal.signalSuccessWhen(messages.getCoder(), new NonEmptyMessageIdCheck()));

    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(5));
    pipeline.apply(signal.signalStart());
    PipelineResult job = pipeline.run();
    start.get();

    signal.waitForSuccess(Duration.standardMinutes(5));
    // A runner may not support cancel
    try {
      job.cancel();
    } catch (UnsupportedOperationException exc) {
      // noop
    }
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
