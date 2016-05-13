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

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubUnboundedSource.PubsubReader;
import org.apache.beam.sdk.io.PubsubUnboundedSource.PubsubSource;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubTestClient;

import com.google.api.client.util.Clock;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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

  @Test
  public void readOneMessage() throws IOException {
    List<IncomingMessage> incoming =
        Lists.newArrayList(new IncomingMessage(DATA.getBytes(), TIMESTAMP, REQ_TIME, ACK_ID,
                                               RECORD_ID.getBytes()));
    final AtomicLong now = new AtomicLong(REQ_TIME);
    Clock clock = new Clock() {
      @Override
      public long currentTimeMillis() {
        return now.get();
      }
    };
    PubsubClientFactory factory =
        PubsubTestClient.createFactoryForPull(clock, SUBSCRIPTION, ACK_TIMEOUT_S, incoming);
    PubsubUnboundedSource<String> source =
        new PubsubUnboundedSource<>(clock, factory, SUBSCRIPTION, StringUtf8Coder.of(),
                                    TIMESTAMP_LABEL, ID_LABEL);
    PubsubSource<String> primSource = new PubsubSource<>(source);

    TestPipeline p = TestPipeline.create();
    PubsubReader<String> reader = primSource.createReader(p.getOptions(), null);
    PubsubTestClient pubsubClient = (PubsubTestClient) reader.getPubsubClient();

    assertTrue(reader.start());
    now.addAndGet(55 * 1000);
    pubsubClient.advance();
    assertEquals(DATA, reader.getCurrent());
    assertFalse(reader.advance());
  }
}
