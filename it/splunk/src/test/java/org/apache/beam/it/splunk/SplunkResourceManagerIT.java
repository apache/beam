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
package org.apache.beam.it.splunk;

import static org.apache.beam.it.splunk.matchers.SplunkAsserts.assertThatSplunkEvents;
import static org.apache.beam.it.splunk.matchers.SplunkAsserts.splunkEventsToRecords;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.splunk.conditions.SplunkEventsCheck;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for Splunk Resource Managers. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class SplunkResourceManagerIT {
  private static final String TEST_ID = "dummy-test";
  private static final int NUM_EVENTS = 100;

  private SplunkResourceManager splunkResourceManager;

  @Before
  public void setUp() {
    splunkResourceManager = SplunkResourceManager.builder(TEST_ID).build();
  }

  @Test
  public void testDefaultSplunkResourceManagerE2E() {
    // Arrange
    String source = RandomStringUtils.randomAlphabetic(1, 20);
    String host = RandomStringUtils.randomAlphabetic(1, 20);
    String sourceType = RandomStringUtils.randomAlphabetic(1, 20);
    List<SplunkEvent> httpEventsSent = generateHttpEvents(source, sourceType, host);

    splunkResourceManager.sendHttpEvents(httpEventsSent);

    // Act
    String query = "search source=" + source + " sourcetype=" + sourceType + " host=" + host;
    await("Retrieving events from Splunk")
        .atMost(Duration.ofMinutes(1))
        .pollInterval(Duration.ofMillis(500))
        .until(
            () ->
                SplunkEventsCheck.builder(splunkResourceManager)
                    .setQuery(query)
                    .setMinEvents(httpEventsSent.size())
                    .build()
                    .get());

    List<SplunkEvent> httpEventsReceived = splunkResourceManager.getEvents(query);

    // Assert
    assertThatSplunkEvents(httpEventsReceived)
        .hasRecordsUnordered(splunkEventsToRecords(httpEventsSent));
  }

  private static List<SplunkEvent> generateHttpEvents(
      String source, String sourceType, String host) {
    List<SplunkEvent> events = new ArrayList<>();
    long currentTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_EVENTS; i++) {
      String event = RandomStringUtils.randomAlphabetic(1, 20);
      events.add(
          SplunkEvent.newBuilder()
              .withEvent(event)
              .withSource(source)
              .withSourceType(sourceType)
              .withHost(host)
              .withTime(currentTime + i)
              .create());
    }

    return events;
  }
}
