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
package org.apache.beam.sdk.io.splunk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.gson.JsonObject;
import org.junit.Test;

/** Unit tests for {@link SplunkEvent} class. */
public class SplunkEventTest {

  @Test
  public void testEquals() {

    String event = "test-event";
    String host = "test-host";
    String index = "test-index";
    String source = "test-source";
    String sourceType = "test-source-type";
    Long time = 123456789L;
    JsonObject fields = new JsonObject();
    fields.addProperty("test-key", "test-value");

    SplunkEvent actualEvent =
        SplunkEvent.newBuilder()
            .withEvent(event)
            .withHost(host)
            .withIndex(index)
            .withSource(source)
            .withSourceType(sourceType)
            .withTime(time)
            .withFields(fields)
            .create();

    assertEquals(
        SplunkEvent.newBuilder()
            .withEvent(event)
            .withHost(host)
            .withIndex(index)
            .withSource(source)
            .withSourceType(sourceType)
            .withTime(time)
            .withFields(fields)
            .create(),
        actualEvent);

    assertNotEquals(
        SplunkEvent.newBuilder()
            .withEvent("a-different-test-event")
            .withHost(host)
            .withIndex(index)
            .withSource(source)
            .withSourceType(sourceType)
            .withTime(time)
            .create(),
        actualEvent);
  }
}
