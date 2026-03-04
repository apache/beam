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
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

/** Unit tests for {@link DatadogWriteError} class. */
public class DatadogWriteErrorTest {

  /** Test whether a {@link DatadogWriteError} created via its builder can be compared correctly. */
  @Test
  public void testEquals() {

    String payload = "test-payload";
    String message = "test-message";
    Integer statusCode = 123;

    DatadogWriteError actualError =
        DatadogWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage(message)
            .build();

    assertThat(
        actualError,
        is(
            equalTo(
                DatadogWriteError.newBuilder()
                    .withPayload(payload)
                    .withStatusCode(statusCode)
                    .withStatusMessage(message)
                    .build())));

    assertThat(
        actualError,
        is(
            not(
                equalTo(
                    DatadogWriteError.newBuilder()
                        .withPayload(payload)
                        .withStatusCode(statusCode)
                        .withStatusMessage("a-different-message")
                        .build()))));
  }
}
