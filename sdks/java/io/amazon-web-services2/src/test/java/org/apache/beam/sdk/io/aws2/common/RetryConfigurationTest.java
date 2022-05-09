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
package org.apache.beam.sdk.io.aws2.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.Duration.ZERO;

import org.apache.beam.sdk.io.aws2.options.SerializationTestUtil;
import org.joda.time.Duration;
import org.junit.Test;

public class RetryConfigurationTest {
  @Test
  public void verifyNumRetriesNotNegative() {
    assertThatThrownBy(() -> RetryConfiguration.builder().numRetries(-1).build())
        .hasMessage("numRetries must not be negative");
  }

  @Test
  public void verifyBaseBackoffLargerZero() {
    assertThatThrownBy(() -> RetryConfiguration.builder().numRetries(1).baseBackoff(ZERO).build())
        .hasMessage("baseBackoff must be greater than 0");
  }

  @Test
  public void verifyThrottledBaseBackoffLargerZero() {
    assertThatThrownBy(
            () -> RetryConfiguration.builder().numRetries(1).throttledBaseBackoff(ZERO).build())
        .hasMessage("throttledBaseBackoff must be greater than 0");
  }

  @Test
  public void verifyMaxBackoffLargerZero() {
    assertThatThrownBy(() -> RetryConfiguration.builder().numRetries(1).maxBackoff(ZERO).build())
        .hasMessage("maxBackoff must be greater than 0");
  }

  @Test
  public void testJsonSerialization() {
    RetryConfiguration config = RetryConfiguration.builder().numRetries(10).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().maxBackoff(Duration.millis(1000)).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().baseBackoff(Duration.millis(200)).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().throttledBaseBackoff(Duration.millis(100)).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);
  }

  private RetryConfiguration jsonSerializeDeserialize(RetryConfiguration obj) {
    return SerializationTestUtil.serializeDeserialize(RetryConfiguration.class, obj);
  }
}
