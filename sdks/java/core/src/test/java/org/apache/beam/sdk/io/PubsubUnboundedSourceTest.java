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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubTestClient;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Test PubsubUnboundedSource.
 */
@RunWith(JUnit4.class)
public class PubsubUnboundedSourceTest {
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final String DATA = "testData";
  private static final long TIMESTAMP = 1234L;
  private static final long REQ_TIME = 6373L;
  private static final String TIMESTAMP_LABEL = "timestamp";
  private static final String ID_LABEL = "id";
  private static final String ACK_ID = "testAckId";
  private static final String RECORD_ID = "testRecordId";
  private static final int ACK_TIMEOUT_S = 60;

  private static class CheckStamp extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      assertEquals(TIMESTAMP, c.timestamp().getMillis());
      c.output(c.element());
    }
  }

  @Test
  public void receiveOneMessage() {
    // TODO: DISABLED until TestPipeline/direct runner supports unbounded sources.
    if (false) {
      List<IncomingMessage> incoming =
          Lists.newArrayList(new IncomingMessage(DATA.getBytes(), TIMESTAMP, REQ_TIME, ACK_ID,
                                                 RECORD_ID.getBytes()));
      PubsubClientFactory factory =
          PubsubTestClient.createFactoryForPull(SUBSCRIPTION, ACK_TIMEOUT_S, incoming);
      PubsubUnboundedSource<String> source =
          new PubsubUnboundedSource<>(factory, SUBSCRIPTION, StringUtf8Coder.of(), TIMESTAMP_LABEL,
                                      ID_LABEL);
      TestPipeline p = TestPipeline.create();
      PCollection<String> strings =
          p.apply(source)
           .apply(Window.<String>into(new GlobalWindows())
                      .triggering(AfterPane.elementCountAtLeast(1))
                      .discardingFiredPanes())
           .apply(ParDo.of(new CheckStamp()));
      PAssert.that(strings).containsInAnyOrder(DATA);
      p.run();
    }
  }
}
