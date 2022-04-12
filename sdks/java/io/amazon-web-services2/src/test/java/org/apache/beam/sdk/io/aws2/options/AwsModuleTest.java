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

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SECRET_ACCESS_KEY;

import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/** Tests {@link AwsModule}. */
@RunWith(JUnit4.class)
public class AwsModuleTest {

  @Test
  public void testObjectMapperIsAbleToFindModule() {
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    MatcherAssert.assertThat(modules, hasItem(instanceOf(AwsModule.class)));
  }

  private <T> T serializeAndDeserialize(T obj) {
    return SerializationTestUtil.serializeDeserialize((Class<T>) obj.getClass(), obj);
  }

  @Test
  public void testStaticCredentialsProviderSerializationDeserialization() {
    AwsCredentialsProvider provider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret"));

    assertThat(serializeAndDeserialize(provider))
        .hasSameClassAs(provider)
        .isEqualToComparingFieldByFieldRecursively(provider);

    provider =
        StaticCredentialsProvider.create(AwsSessionCredentials.create("key", "secret", "token"));
    assertThat(serializeAndDeserialize(provider))
        .hasSameClassAs(provider)
        .isEqualToComparingFieldByFieldRecursively(provider);
  }

  @Test
  public void testAwsCredentialsProviderSerializationDeserialization() {
    AwsCredentialsProvider provider = DefaultCredentialsProvider.create();
    AwsCredentialsProvider deserializedProvider = serializeAndDeserialize(provider);
    assertEquals(provider.getClass(), deserializedProvider.getClass());

    provider = EnvironmentVariableCredentialsProvider.create();
    deserializedProvider = serializeAndDeserialize(provider);
    assertEquals(provider.getClass(), deserializedProvider.getClass());

    provider = SystemPropertyCredentialsProvider.create();
    deserializedProvider = serializeAndDeserialize(provider);
    assertEquals(provider.getClass(), deserializedProvider.getClass());

    provider = ProfileCredentialsProvider.create();
    deserializedProvider = serializeAndDeserialize(provider);
    assertEquals(provider.getClass(), deserializedProvider.getClass());

    provider = ContainerCredentialsProvider.builder().build();
    deserializedProvider = serializeAndDeserialize(provider);
    assertEquals(provider.getClass(), deserializedProvider.getClass());
  }

  @Test
  public void testStsAssumeRoleCredentialsProviderSerializationDeserialization() throws Exception {
    AssumeRoleRequest req = AssumeRoleRequest.builder().roleArn("roleArn").policy("policy").build();
    Supplier<AwsCredentialsProvider> provider =
        () ->
            StsAssumeRoleCredentialsProvider.builder()
                .stsClient(StsClient.create())
                .refreshRequest(req)
                .build();

    Properties overrides = new Properties();
    overrides.setProperty(AWS_REGION.property(), Regions.US_EAST_1.getName());
    overrides.setProperty(AWS_ACCESS_KEY_ID.property(), "key");
    overrides.setProperty(AWS_SECRET_ACCESS_KEY.property(), "secret");

    // Region and credentials for STS client are resolved using default providers
    AwsCredentialsProvider deserializedProvider =
        withSystemPropertyOverrides(overrides, () -> serializeAndDeserialize(provider.get()));

    Supplier<AssumeRoleRequest> requestSupplier =
        (Supplier<AssumeRoleRequest>)
            readField(deserializedProvider, "assumeRoleRequestSupplier", true);
    assertThat(requestSupplier.get()).isEqualTo(req);
  }

  @Test
  public void testProxyConfigurationSerializationDeserialization() {
    ProxyConfiguration proxyConfiguration =
        ProxyConfiguration.builder()
            .endpoint(URI.create("http://localhost:8080"))
            .username("username")
            .password("password")
            .build();

    ProxyConfiguration deserializedProxyConfiguration = serializeAndDeserialize(proxyConfiguration);
    assertEquals("localhost", deserializedProxyConfiguration.host());
    assertEquals(8080, deserializedProxyConfiguration.port());
    assertEquals("username", deserializedProxyConfiguration.username());
    assertEquals("password", deserializedProxyConfiguration.password());
  }

  private <T> T withSystemPropertyOverrides(Properties overrides, ThrowingSupplier<T> fun)
      throws Exception {
    Properties systemProps = System.getProperties();

    Properties previousProps = new Properties();
    systemProps.entrySet().stream()
        .filter(e -> overrides.containsKey(e.getKey()))
        .forEach(e -> previousProps.put(e.getKey(), e.getValue()));

    overrides.forEach(systemProps::put);
    try {
      return fun.get();
    } finally {
      overrides.forEach(systemProps::remove);
      previousProps.forEach(systemProps::put);
    }
  }
}
