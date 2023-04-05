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

import java.net.URI;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.aws2.common.HttpClientConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;

public class AwsOptionsTest {

  private AwsOptions serializeDeserialize(AwsOptions opts) {
    return SerializationTestUtil.serializeDeserialize(PipelineOptions.class, opts)
        .as(AwsOptions.class);
  }

  private AwsOptions create(String... args) {
    return PipelineOptionsFactory.fromArgs(args).as(AwsOptions.class);
  }

  @Test
  public void testSerializeDeserializeDefaults() {
    AwsOptions options = create();

    // trigger factories
    assertThat(withRegionProperty(Region.US_WEST_1, () -> options.getAwsRegion()))
        .isEqualTo(Region.US_WEST_1);
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

    assertThat(copy.getHttpClientConfiguration()).isNull();
    assertThat(options.getHttpClientConfiguration()).isNull();
  }

  @Test
  public void testSetAwsRegion() {
    AwsOptions options = create("--awsRegion=us-west-1");
    assertThat(options.getAwsRegion()).isEqualTo(Region.US_WEST_1);
    assertThat(serializeDeserialize(options).getAwsRegion()).isEqualTo(Region.US_WEST_1);
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
    assertThat(options.getEndpoint()).isEqualTo(URI.create("https://localhost:8080"));
    assertThat(serializeDeserialize(options).getEndpoint())
        .isEqualTo(URI.create("https://localhost:8080"));
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
  public void testSetHttpClientConfiguration() {
    AwsOptions options =
        create(
            "--httpClientConfiguration={"
                + "\"connectionAcquisitionTimeout\":100,"
                + "\"connectionMaxIdleTime\":200,"
                + "\"connectionTimeout\":300,"
                + "\"connectionTimeToLive\":400,"
                + "\"socketTimeout\":500,"
                + "\"readTimeout\":600,"
                + "\"writeTimeout\":700,"
                + "\"maxConnections\":10}");

    HttpClientConfiguration expected =
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

    assertThat(options.getHttpClientConfiguration()).isEqualTo(expected);
    assertThat(serializeDeserialize(options).getHttpClientConfiguration()).isEqualTo(expected);
  }

  private <T> T withRegionProperty(Region region, Supplier<T> fun) {
    String oldRegion = System.getProperty(AWS_REGION.property());
    System.setProperty(AWS_REGION.property(), region.id());
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
