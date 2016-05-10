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

package org.apache.beam.sdk.io;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.util.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.util.PubsubClient.TopicPath;
import org.apache.beam.sdk.util.PubsubTestClient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

/**
 * Test PubsubUnboundedSink.
 */
@RunWith(JUnit4.class)
public class PubsubUnboundedSinkTest {
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final String DATA = "testData";
  private static final long TIMESTAMP = 1234L;
  private static final String TIMESTAMP_LABEL = "timestamp";
  private static final String ID_LABEL = "id";

  private static class Stamp extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.outputWithTimestamp(c.element(), new Instant(TIMESTAMP));
    }
  }

  @Test
  public void saneCoder() throws Exception {
    OutgoingMessage message = new OutgoingMessage(DATA.getBytes(), TIMESTAMP);
    CoderProperties.coderDecodeEncodeEqual(PubsubUnboundedSink.CODER, message);
    CoderProperties.coderSerializable(PubsubUnboundedSink.CODER);
  }

  @Test
  public void sendOneMessage() {
    Set<OutgoingMessage> outgoing =
        Sets.newHashSet(new OutgoingMessage(DATA.getBytes(), TIMESTAMP));
    PubsubClientFactory factory =
        PubsubTestClient.createFactoryForPublish(TOPIC, outgoing);
    PubsubUnboundedSink<String> sink =
        new PubsubUnboundedSink<>(factory, TOPIC, StringUtf8Coder.of(), TIMESTAMP_LABEL, ID_LABEL,
                                  10);
    TestPipeline p = TestPipeline.create();
    p.apply(Create.of(ImmutableList.of(DATA)))
     .apply(ParDo.of(new Stamp()))
     .apply(sink);
    p.run();
  }
}
