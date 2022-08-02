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

import static org.apache.beam.sdk.util.SerializableUtils.deserializeFromByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.apache.beam.sdk.io.aws2.options.SerializationTestUtil;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class ClientConfigurationTest {

  @Test
  public void testJavaSerialization() {
    AwsCredentialsProvider credentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret"));

    ClientConfiguration config =
        ClientConfiguration.builder()
            .credentialsProvider(credentials)
            .region(Region.US_WEST_1)
            .endpoint(URI.create("https://localhost"))
            .retry(b -> b.numRetries(3))
            .build();

    byte[] serializedBytes = serializeToByteArray(config);

    ClientConfiguration deserializedConfig =
        (ClientConfiguration) deserializeFromByteArray(serializedBytes, "ClientConfiguration");

    assertThat(deserializedConfig).isEqualTo(config);
  }

  @Test
  public void testJsonSerialization() {
    ClientConfiguration config = ClientConfiguration.builder().build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().region(Region.US_WEST_1).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().credentialsProvider(DefaultCredentialsProvider.create()).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
    config = config.toBuilder().credentialsProvider(credentialsProvider).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().endpoint(URI.create("https://localhost:8080")).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);

    config = config.toBuilder().retry(r -> r.numRetries(10)).build();
    assertThat(jsonSerializeDeserialize(config)).isEqualTo(config);
  }

  private ClientConfiguration jsonSerializeDeserialize(ClientConfiguration obj) {
    return SerializationTestUtil.serializeDeserialize(ClientConfiguration.class, obj);
  }
}
