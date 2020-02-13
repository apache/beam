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

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

/** Tests {@link AwsModule}. */
@RunWith(JUnit4.class)
public class AwsModuleTest {
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new AwsModule());

  @Test
  public void testObjectMapperIsAbleToFindModule() {
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    assertThat(modules, hasItem(Matchers.instanceOf(AwsModule.class)));
  }

  @Test
  public void testStaticCredentialsProviderSerializationDeserialization() throws Exception {
    AwsCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("key-id", "secret-key"));
    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AwsCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AwsCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    assertEquals(
        credentialsProvider.resolveCredentials().accessKeyId(),
        deserializedCredentialsProvider.resolveCredentials().accessKeyId());
    assertEquals(
        credentialsProvider.resolveCredentials().secretAccessKey(),
        deserializedCredentialsProvider.resolveCredentials().secretAccessKey());
  }

  @Test
  public void testAwsCredentialsProviderSerializationDeserialization() throws Exception {
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AwsCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, DefaultCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = EnvironmentVariableCredentialsProvider.create();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AwsCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = SystemPropertyCredentialsProvider.create();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AwsCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = ProfileCredentialsProvider.create();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AwsCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = ContainerCredentialsProvider.builder().build();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AwsCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
  }

  @Test
  public void testProxyConfigurationSerializationDeserialization() throws Exception {
    ProxyConfiguration proxyConfiguration =
        ProxyConfiguration.builder()
            .endpoint(URI.create("http://localhost:8080"))
            .username("username")
            .password("password")
            .build();
    String valueAsJson = objectMapper.writeValueAsString(proxyConfiguration);
    ProxyConfiguration deserializedProxyConfiguration =
        objectMapper.readValue(valueAsJson, ProxyConfiguration.class);
    assertEquals("localhost", deserializedProxyConfiguration.host());
    assertEquals(8080, deserializedProxyConfiguration.port());
    assertEquals("username", deserializedProxyConfiguration.username());
    assertEquals("password", deserializedProxyConfiguration.password());
  }
}
