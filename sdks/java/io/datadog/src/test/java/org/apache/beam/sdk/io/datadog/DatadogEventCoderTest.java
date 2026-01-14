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
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.datadog.DatadogEventCoder} class. */
public class DatadogEventCoderTest {

  /**
   * Test whether {@link DatadogEventCoder} is able to encode/decode a {@link DatadogEvent}
   * correctly.
   *
   * @throws IOException
   */
  @Test
  public void testEncodeDecode() throws IOException {

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

    DatadogEventCoder coder = DatadogEventCoder.of();
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      coder.encode(actualEvent, bos);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())) {
        DatadogEvent decodedEvent = coder.decode(bin);
        assertThat(decodedEvent, is(equalTo(actualEvent)));
      }
    }
  }
}
