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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;

import java.net.URI;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.aws2.auth.FakeSecretRegistrar;
import org.apache.beam.sdk.io.aws2.auth.SecretCredentialsProvider;
import org.apache.beam.sdk.io.aws2.common.HttpClientConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
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

  @Test
  public void testSetAwsRegionName() {
    AwsOptions options = create("--awsRegionName=us-west-1");
    assertThat(options.getAwsRegionName()).isEqualTo("us-west-1");
    assertThat(options.getAwsRegion()).isEqualTo(Region.US_WEST_1);
    assertThat(AwsOptionsResolver.region(options)).isEqualTo(Region.US_WEST_1);
    assertThat(AwsOptionsResolver.region(serializeDeserialize(options)))
        .isEqualTo(Region.US_WEST_1);
  }

  @Test
  public void testConflictingRegions() {
    AwsOptions same = create("--awsRegion=us-west-1", "--awsRegionName=us-west-1");
    assertThat(AwsOptionsResolver.region(same)).isEqualTo(Region.US_WEST_1);

    AwsOptions conflict = create("--awsRegion=us-west-1", "--awsRegionName=us-east-1");
    assertThatThrownBy(() -> AwsOptionsResolver.region(conflict))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Conflicting AWS regions");
  }

  @Test
  public void testSetAwsEndpointUrl() {
    AwsOptions options = create("--awsEndpointUrl=https://localhost:4566");
    URI expected = URI.create("https://localhost:4566");
    assertThat(options.getAwsEndpointUrl()).isEqualTo("https://localhost:4566");
    assertThat(options.getEndpoint()).isEqualTo(expected);
    assertThat(AwsOptionsResolver.endpoint(options)).isEqualTo(expected);
    assertThat(AwsOptionsResolver.endpoint(serializeDeserialize(options))).isEqualTo(expected);
  }

  @Test
  public void testConflictingEndpoints() {
    AwsOptions same =
        create("--endpoint=https://localhost:4566", "--awsEndpointUrl=https://localhost:4566");
    assertThat(AwsOptionsResolver.endpoint(same)).isEqualTo(URI.create("https://localhost:4566"));

    AwsOptions conflict =
        create("--endpoint=https://localhost:4566", "--awsEndpointUrl=https://localhost:8080");
    assertThatThrownBy(() -> AwsOptionsResolver.endpoint(conflict))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Conflicting AWS endpoints");
  }

  @Test
  public void testInvalidAwsEndpointUrl() {
    AwsOptions options = create("--awsEndpointUrl=http://bad host");
    assertThatThrownBy(() -> AwsOptionsResolver.endpoint(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid --awsEndpointUrl");
  }

  @Test
  public void testCredentialsFromRawKeys() {
    AwsOptions options = create("--awsAccessKeyId=key", "--awsSecretAccessKey=secret");
    assertThat(options.getAwsCredentialsProvider())
        .isEqualToComparingFieldByFieldRecursively(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")));
    AwsOptions copy = serializeDeserialize(options);
    assertThat(AwsOptionsResolver.credentialsProvider(copy))
        .isEqualToComparingFieldByFieldRecursively(options.getAwsCredentialsProvider());

    AwsOptions session =
        create("--awsAccessKeyId=key", "--awsSecretAccessKey=secret", "--awsSessionToken=token");
    assertThat(session.getAwsCredentialsProvider())
        .isEqualToComparingFieldByFieldRecursively(
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create("key", "secret", "token")));
  }

  @Test
  public void testCredentialsFromSecretManager() {
    String keySpec = FakeSecretRegistrar.spec("key");
    String secretSpec = FakeSecretRegistrar.spec("secret");
    AwsOptions options =
        create(
            "--awsCredentialsSecretManager=" + FakeSecretRegistrar.NAME,
            "--awsAccessKeyId=" + keySpec,
            "--awsSecretAccessKey=" + secretSpec);
    SecretCredentialsProvider expected =
        SecretCredentialsProvider.create(FakeSecretRegistrar.NAME, keySpec, secretSpec);
    assertThat(options.getAwsCredentialsProvider()).isEqualTo(expected);
    assertThat(AwsOptionsResolver.credentialsProvider(options)).isEqualTo(expected);

    // The derived provider is serialized (as specs only) and stays consistent with the key options.
    AwsOptions copy = serializeDeserialize(options);
    assertThat(AwsOptionsResolver.credentialsProvider(copy)).isEqualTo(expected);
    assertThat(copy.getAwsCredentialsProvider().resolveCredentials().accessKeyId())
        .isEqualTo("key");
  }

  @Test
  public void testIncompleteCredentialOptions() {
    assertThatThrownBy(() -> create("--awsAccessKeyId=key").getAwsCredentialsProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be set together");
    assertThatThrownBy(() -> create("--awsSecretAccessKey=secret").getAwsCredentialsProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be set together");
    assertThatThrownBy(
            () ->
                create("--awsCredentialsSecretManager=GoogleCloudSecretManager")
                    .getAwsCredentialsProvider())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("require --awsAccessKeyId");
  }

  @Test
  public void testAwsSecretsManagerCannotHoldCredentials() {
    AwsOptions options =
        create(
            "--awsCredentialsSecretManager=AwsSecretsManager",
            "--awsAccessKeyId=key",
            "--awsSecretAccessKey=secret");
    assertThatThrownBy(options::getAwsCredentialsProvider)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be AWS Secrets Manager");
  }

  @Test
  public void testCredentialsProviderCannotBeCombinedWithKeys() {
    AwsOptions options =
        create(
            "--awsCredentialsProvider={\"@type\":\"StaticCredentialsProvider\",\"accessKeyId\":\"other\",\"secretAccessKey\":\"other\"}",
            "--awsAccessKeyId=key",
            "--awsSecretAccessKey=secret");
    assertThatThrownBy(() -> AwsOptionsResolver.credentialsProvider(options))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--awsCredentialsProvider cannot be combined");

    AwsOptions withSecretManager =
        create(
            "--awsCredentialsProvider={\"@type\":\"DefaultCredentialsProvider\"}",
            "--awsCredentialsSecretManager=" + FakeSecretRegistrar.NAME,
            "--awsAccessKeyId=" + FakeSecretRegistrar.spec("key"),
            "--awsSecretAccessKey=" + FakeSecretRegistrar.spec("secret"));
    assertThatThrownBy(() -> AwsOptionsResolver.credentialsProvider(withSecretManager))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--awsCredentialsProvider cannot be combined");
  }

  @Test
  public void testPythonCrossLanguageOptions() {
    // Options as sent by the Python SDK (e.g. in an expansion request), which uses the
    // snake_case names of the --aws_* options.
    String keySpec = FakeSecretRegistrar.spec("key");
    String secretSpec = FakeSecretRegistrar.spec("secret");
    String json =
        "{"
            + "\"beam:option:aws_region_name:v1\": \"us-west-1\","
            + "\"beam:option:aws_endpoint_url:v1\": \"https://localhost:4566\","
            + "\"beam:option:aws_credentials_secret_manager:v1\": \""
            + FakeSecretRegistrar.NAME
            + "\","
            + "\"beam:option:aws_access_key_id:v1\": "
            + jsonString(keySpec)
            + ","
            + "\"beam:option:aws_secret_access_key:v1\": "
            + jsonString(secretSpec)
            + "}";
    AwsOptions options = PipelineOptionsTranslation.fromJson(json).as(AwsOptions.class);

    assertThat(AwsOptionsResolver.region(options)).isEqualTo(Region.US_WEST_1);
    assertThat(AwsOptionsResolver.endpoint(options))
        .isEqualTo(URI.create("https://localhost:4566"));
    assertThat(AwsOptionsResolver.credentialsProvider(options))
        .isEqualTo(SecretCredentialsProvider.create(FakeSecretRegistrar.NAME, keySpec, secretSpec));
  }

  private static String jsonString(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
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
