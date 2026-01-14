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
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/** Unit tests for {@link DatadogEvent} class. */
public class DatadogEventTest {

  /** Test whether a {@link DatadogEvent} created via its builder can be compared correctly. */
  @Test
  public void testEquals() {
    String source = "test-source";
    String tags = "test-tags";
    String hostname = "test-hostname";
    String service = "test-service";
    String message = "test-message";

    DatadogEvent actualEvent =
        DatadogEvent.newBuilder()
            .withSource(source)
            .withTags(tags)
            .withHostname(hostname)
            .withService(service)
            .withMessage(message)
            .build();

    assertThat(
        actualEvent,
        is(
            equalTo(
                DatadogEvent.newBuilder()
                    .withSource(source)
                    .withTags(tags)
                    .withHostname(hostname)
                    .withService(service)
                    .withMessage(message)
                    .build())));

    assertThat(
        actualEvent,
        is(
            not(
                equalTo(
                    DatadogEvent.newBuilder()
                        .withSource(source)
                        .withTags(tags)
                        .withHostname(hostname)
                        .withService(service)
                        .withMessage("a-different-test-message")
                        .build()))));
  }
}
