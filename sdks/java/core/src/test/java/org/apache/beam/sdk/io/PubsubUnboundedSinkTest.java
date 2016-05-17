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
import org.apache.beam.sdk.util.PubsubClient.TopicPath;
import org.apache.beam.sdk.util.PubsubTestClient;
import org.apache.beam.sdk.util.PubsubTestClient.PubsubTestClientFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
  public void sendOneMessage() throws IOException {
    Set<OutgoingMessage> outgoing =
        Sets.newHashSet(new OutgoingMessage(DATA.getBytes(), TIMESTAMP));
    try (PubsubTestClientFactory factory =
             PubsubTestClient.createFactoryForPublish(TOPIC, outgoing)) {
      PubsubUnboundedSink<String> sink =
          new PubsubUnboundedSink<>(factory, TOPIC, StringUtf8Coder.of(), TIMESTAMP_LABEL, ID_LABEL,
                                    10);
      TestPipeline p = TestPipeline.create();
      p.apply(Create.of(ImmutableList.of(DATA)))
       .apply(ParDo.of(new Stamp()))
       .apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void sendMoreThanOneBatchByNumMessages() throws IOException {
    Set<OutgoingMessage> outgoing = new HashSet<>();
    List<String> data = new ArrayList<>();
    int batchSize = 2;
    int batchBytes = 1000;
    for (int i = 0; i < batchSize * 10; i++) {
      String str = String.valueOf(i);
      outgoing.add(new OutgoingMessage(str.getBytes(), TIMESTAMP));
      data.add(str);
    }
    try (PubsubTestClientFactory factory =
             PubsubTestClient.createFactoryForPublish(TOPIC, outgoing)) {
      PubsubUnboundedSink<String> sink =
          new PubsubUnboundedSink<>(factory, TOPIC, StringUtf8Coder.of(), TIMESTAMP_LABEL, ID_LABEL,
                                    10, batchSize, batchBytes, Duration.standardSeconds(2));
      TestPipeline p = TestPipeline.create();
      p.apply(Create.of(data))
       .apply(ParDo.of(new Stamp()))
       .apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void sendMoreThanOneBatchByByteSize() throws IOException {
    Set<OutgoingMessage> outgoing = new HashSet<>();
    List<String> data = new ArrayList<>();
    int batchSize = 100;
    int batchBytes = 10;
    int n = 0;
    while (n < batchBytes * 10) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < batchBytes; i++) {
        sb.append(String.valueOf(n));
      }
      String str = sb.toString();
      outgoing.add(new OutgoingMessage(str.getBytes(), TIMESTAMP));
      data.add(str);
      n += str.length();
    }
    try (PubsubTestClientFactory factory =
             PubsubTestClient.createFactoryForPublish(TOPIC, outgoing)) {
      PubsubUnboundedSink<String> sink =
          new PubsubUnboundedSink<>(factory, TOPIC, StringUtf8Coder.of(), TIMESTAMP_LABEL, ID_LABEL,
                                    10, batchSize, batchBytes, Duration.standardSeconds(2));
      TestPipeline p = TestPipeline.create();
      p.apply(Create.of(data))
       .apply(ParDo.of(new Stamp()))
       .apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }
}
