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
package org.apache.beam.sdk.io.aws2.options;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.CONNECTION_MAX_IDLE_TIMEOUT;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.CONNECTION_TIMEOUT;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.CONNECTION_TIME_TO_LIVE;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.MAX_CONNECTIONS;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.READ_TIMEOUT;

import java.net.URI;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.beam.repackaged.direct_java.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

public class AwsOptionsTest {
  private AwsOptions serializeDeserialize(AwsOptions opts) {
    SerializablePipelineOptions serialized = new SerializablePipelineOptions(opts);
    return SerializableUtils.clone(serialized).get().as(AwsOptions.class);
  }

  private AwsOptions create(String... args) {
    return PipelineOptionsFactory.fromArgs(args).as(AwsOptions.class);
  }

  @Test
  public void testSerializeDeserializeDefaults() {
    AwsOptions options = create();

    // trigger factories
    assertThat(withRegionProperty("us-west-1", () -> options.getAwsRegion()))
        .isEqualTo("us-west-1");
    assertThat(options.getAwsCredentialsProvider())
        .isEqualTo(DefaultCredentialsProvider.create()); // by instance

    AwsOptions copy = serializeDeserialize(options);
    assertThat(copy.getAwsRegion()).isEqualTo(options.getAwsRegion());
    assertThat(copy.getAwsCredentialsProvider())
        .isEqualTo(options.getAwsCredentialsProvider()); // by instance

    assertThat(copy.getEndpoint()).isNull();
    assertThat(options.getEndpoint()).isNull();

    assertThat(copy.getProxyConfiguration()).isNull();
    assertThat(options.getProxyConfiguration()).isNull();

    assertThat(copy.getAttributeMap()).isNull();
    assertThat(options.getAttributeMap()).isNull();
  }

  @Test
  public void testSetAwsRegion() {
    AwsOptions options = create("--awsRegion=us-west-1");
    assertThat(options.getAwsRegion()).isEqualTo("us-west-1");
    assertThat(serializeDeserialize(options).getAwsRegion()).isEqualTo("us-west-1");
  }

  @Test
  public void testSetAwsCredentialsProvider() {
    AwsOptions options =
        create(
            "--awsCredentialsProvider={\"@type\":\"StaticCredentialsProvider\",\"accessKeyId\":\"key\",\"secretAccessKey\":\"secret\"}");
    AwsCredentialsProvider expected =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret"));
    assertThat(options.getAwsCredentialsProvider())
        .isEqualToComparingFieldByFieldRecursively(expected);
    assertThat(serializeDeserialize(options).getAwsCredentialsProvider())
        .isEqualToComparingFieldByFieldRecursively(expected);
  }

  @Test
  public void testSetEndpoint() {
    AwsOptions options = create("--endpoint=https://localhost:8080");
    assertThat(options.getEndpoint()).isEqualTo("https://localhost:8080");
    assertThat(serializeDeserialize(options).getEndpoint()).isEqualTo("https://localhost:8080");
  }

  @Test
  public void testSetProxyConfiguration() {
    AwsOptions options =
        create(
            "--proxyConfiguration={\"endpoint\":\"https://localhost:8080\", \"username\":\"user\", \"password\":\"pw\"}");
    ProxyConfiguration expected =
        ProxyConfiguration.builder()
            .endpoint(URI.create("https://localhost:8080"))
            .username("user")
            .password("pw")
            .build();
    assertThat(options.getProxyConfiguration())
        .isEqualToIgnoringGivenFields(expected, "useSystemPropertyValues");
    assertThat(serializeDeserialize(options).getProxyConfiguration())
        .isEqualToIgnoringGivenFields(expected, "useSystemPropertyValues");
  }

  @Test
  public void testSetAttributeMap() {
    AwsOptions options =
        create(
            "--attributeMap={"
                + "\"connectionAcquisitionTimeout\":\"PT1000S\","
                + "\"connectionMaxIdleTime\":\"PT3000S\","
                + "\"connectionTimeout\":\"PT10000S\","
                + "\"connectionTimeToLive\":\"PT10000S\","
                + "\"maxConnections\":\"10\","
                + "\"socketTimeout\":\"PT5000S\"}");

    AttributeMap expected =
        AttributeMap.builder()
            .put(CONNECTION_ACQUIRE_TIMEOUT, Duration.parse("PT1000S"))
            .put(CONNECTION_MAX_IDLE_TIMEOUT, Duration.parse("PT3000S"))
            .put(CONNECTION_TIMEOUT, Duration.parse("PT10000S"))
            .put(CONNECTION_TIME_TO_LIVE, Duration.parse("PT10000S"))
            .put(MAX_CONNECTIONS, 10)
            .put(READ_TIMEOUT, Duration.parse("PT5000S"))
            .build();

    assertThat(options.getAttributeMap()).isEqualTo(expected);
    assertThat(serializeDeserialize(options).getAttributeMap()).isEqualTo(expected);
  }

  private <T> T withRegionProperty(String region, Supplier<T> fun) {
    String oldRegion = System.getProperty(AWS_REGION.property());
    System.setProperty(AWS_REGION.property(), region);
    try {
      return fun.get();
    } finally {
      if (oldRegion == null) {
        System.clearProperty(AWS_REGION.property());
      } else {
        System.setProperty(AWS_REGION.property(), oldRegion);
      }
    }
  }
}
