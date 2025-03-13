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

import org.apache.beam.sdk.io.aws2.options.SerializationTestUtil;
import org.junit.Test;

public class HttpClientConfigurationTest {
  @Test
  public void testJsonSerialization() {
    HttpClientConfiguration expected = HttpClientConfiguration.builder().build();
    assertThat(serializeAndDeserialize(expected)).isEqualTo(expected);

    expected =
        HttpClientConfiguration.builder()
            .connectionAcquisitionTimeout(100)
            .connectionMaxIdleTime(200)
            .connectionTimeout(300)
            .connectionTimeToLive(400)
            .socketTimeout(500)
            .readTimeout(600)
            .writeTimeout(700)
            .maxConnections(10)
            .build();

    assertThat(serializeAndDeserialize(expected)).isEqualTo(expected);
  }

  private HttpClientConfiguration serializeAndDeserialize(HttpClientConfiguration obj) {
    return SerializationTestUtil.serializeDeserialize(HttpClientConfiguration.class, obj);
  }
}
