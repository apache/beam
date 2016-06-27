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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.display.DisplayData;

import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PubsubIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class PubsubIOTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPubsubIOGetName() {
    assertEquals("PubsubIO.Read",
        PubsubIO.Read.topic("projects/myproject/topics/mytopic").getName());
    assertEquals("PubsubIO.Write",
        PubsubIO.Write.topic("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.Read.topic("projects/my-project/topics/abc");
    PubsubIO.Read.topic("projects/my-project/topics/ABC");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-DeF");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("11111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("1111111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testReadDisplayData() {
    String topic = "projects/project/topics/topic";
    String subscription = "projects/project/subscriptions/subscription";
    Duration maxReadTime = Duration.standardMinutes(5);
    PubsubIO.Read.Bound<String> read = PubsubIO.Read
        .topic(topic)
        .subscription(subscription)
        .timestampLabel("myTimestamp")
        .idLabel("myId")
        .maxNumRecords(1234)
        .maxReadTime(maxReadTime);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("subscription", subscription));
    assertThat(displayData, hasDisplayItem("timestampLabel", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idLabel", "myId"));
    assertThat(displayData, hasDisplayItem("maxNumRecords", 1234));
    assertThat(displayData, hasDisplayItem("maxReadTime", maxReadTime));
  }

  @Test
  public void testWriteDisplayData() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Write.Bound<?> write = PubsubIO.Write
        .topic(topic)
        .timestampLabel("myTimestamp")
        .idLabel("myId");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("timestampLabel", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idLabel", "myId"));
  }
}
