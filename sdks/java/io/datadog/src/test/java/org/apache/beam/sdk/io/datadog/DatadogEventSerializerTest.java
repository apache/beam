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
package org.apache.beam.sdk.io.datadog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class DatadogEventSerializerTest {

  private static final DatadogEvent DATADOG_TEST_EVENT_1 =
      DatadogEvent.newBuilder()
          .withSource("test-source-1")
          .withTags("test-tags-1")
          .withHostname("test-hostname-1")
          .withService("test-service-1")
          .withMessage("test-message-1")
          .build();

  private static final DatadogEvent DATADOG_TEST_EVENT_2 =
      DatadogEvent.newBuilder()
          .withSource("test-source-2")
          .withTags("test-tags-2")
          .withHostname("test-hostname-2")
          .withService("test-service-2")
          .withMessage("test-message-2")
          .build();

  private static final List<DatadogEvent> DATADOG_EVENTS =
      ImmutableList.of(DATADOG_TEST_EVENT_1, DATADOG_TEST_EVENT_2);

  /** Test whether payload is stringified as expected. */
  @Test
  public void stringPayloadTest_list() {
    String actual = DatadogEventSerializer.getPayloadString(DATADOG_EVENTS);

    String expected =
        "["
            + "{\"ddsource\":\"test-source-1\",\"ddtags\":\"test-tags-1\","
            + "\"hostname\":\"test-hostname-1\",\"service\":\"test-service-1\","
            + "\"message\":\"test-message-1\"},"
            + "{\"ddsource\":\"test-source-2\",\"ddtags\":\"test-tags-2\","
            + "\"hostname\":\"test-hostname-2\",\"service\":\"test-service-2\","
            + "\"message\":\"test-message-2\"}"
            + "]";

    assertThat(expected, is(equalTo(actual)));
  }

  /** Test whether payload is stringified as expected. */
  @Test
  public void stringPayloadTest_single() {
    String actual = DatadogEventSerializer.getPayloadString(DATADOG_TEST_EVENT_1);

    String expected =
        "{\"ddsource\":\"test-source-1\",\"ddtags\":\"test-tags-1\","
            + "\"hostname\":\"test-hostname-1\",\"service\":\"test-service-1\","
            + "\"message\":\"test-message-1\"}";

    assertThat(expected, is(equalTo(actual)));
  }

  /** Test payload size calculation for a payload string. */
  @Test
  public void stringPayloadSizeTest() {
    long actual =
        DatadogEventSerializer.getPayloadSize(
            "{\"ddsource\":\"test-source-1\",\"ddtags\":\"test-tags-1\","
                + "\"hostname\":\"test-hostname-1\",\"service\":\"test-service-1\","
                + "\"message\":\"test-message-1\"}");

    long expected = 134L;

    assertThat(expected, is(equalTo(actual)));
  }
}
