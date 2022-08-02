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
package org.apache.beam.sdk.io.kinesis.serde;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link AwsModule}. */
@RunWith(JUnit4.class)
public class AwsModuleTest {
  private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";
  private static final String SESSION_TOKEN = "SESSION_TOKEN";

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new AwsModule());

  private String serialize(Object obj) throws Exception {
    return objectMapper.writeValueAsString(obj);
  }

  private <T> T deserialize(String serializedObj, Class<T> clazz) throws Exception {
    return objectMapper.readValue(serializedObj, clazz);
  }

  private AWSCredentialsProvider deserializeCredentialsProvider(String serializedProvider)
      throws Exception {
    return deserialize(serializedProvider, AWSCredentialsProvider.class);
  }

  @Test
  public void testObjectMapperCannotFindModule() {
    // module shall not be discoverable to not conflict with the one in amazon-web-services
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    assertThat(modules, not(hasItem(Matchers.instanceOf(AwsModule.class))));
  }

  private void checkStaticBasicCredentials(AWSCredentialsProvider provider) {
    assertEquals(AWSStaticCredentialsProvider.class, provider.getClass());
    assertEquals(ACCESS_KEY_ID, provider.getCredentials().getAWSAccessKeyId());
    assertEquals(SECRET_ACCESS_KEY, provider.getCredentials().getAWSSecretKey());
  }

  private void checkStaticSessionCredentials(AWSCredentialsProvider provider) {
    checkStaticBasicCredentials(provider);
    assertEquals(
        SESSION_TOKEN, ((AWSSessionCredentials) provider.getCredentials()).getSessionToken());
  }

  @Test
  public void testAWSStaticCredentialsProviderSerializationDeserialization() throws Exception {
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));

    String serializedCredentialsProvider = serialize(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        deserializeCredentialsProvider(serializedCredentialsProvider);

    checkStaticBasicCredentials(deserializedCredentialsProvider);

    credentialsProvider =
        new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN));

    checkStaticSessionCredentials(credentialsProvider);
  }

  @Test
  public void testPropertiesFileCredentialsProviderSerializationDeserialization() throws Exception {
    String credentialsFilePath = "/path/to/file";

    AWSCredentialsProvider credentialsProvider =
        new PropertiesFileCredentialsProvider(credentialsFilePath);

    String serializedCredentialsProvider = serialize(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        deserializeCredentialsProvider(serializedCredentialsProvider);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    assertEquals(
        credentialsFilePath,
        readField(deserializedCredentialsProvider, "credentialsFilePath", true));
  }

  @Test
  public void testClasspathPropertiesFileCredentialsProviderSerializationDeserialization()
      throws Exception {
    String credentialsFilePath = "/path/to/file";

    AWSCredentialsProvider credentialsProvider =
        new ClasspathPropertiesFileCredentialsProvider(credentialsFilePath);

    String serializedCredentialsProvider = serialize(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        deserializeCredentialsProvider(serializedCredentialsProvider);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    assertEquals(
        credentialsFilePath,
        readField(deserializedCredentialsProvider, "credentialsFilePath", true));
  }

  @Test
  public void testSingletonAWSCredentialsProviderSerializationDeserialization() throws Exception {
    AWSCredentialsProvider credentialsProvider;
    String serializedCredentialsProvider;
    AWSCredentialsProvider deserializedCredentialsProvider;

    credentialsProvider = new DefaultAWSCredentialsProviderChain();
    serializedCredentialsProvider = serialize(credentialsProvider);
    deserializedCredentialsProvider = deserializeCredentialsProvider(serializedCredentialsProvider);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new EnvironmentVariableCredentialsProvider();
    serializedCredentialsProvider = serialize(credentialsProvider);
    deserializedCredentialsProvider = deserializeCredentialsProvider(serializedCredentialsProvider);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new SystemPropertiesCredentialsProvider();
    serializedCredentialsProvider = serialize(credentialsProvider);
    deserializedCredentialsProvider = deserializeCredentialsProvider(serializedCredentialsProvider);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new ProfileCredentialsProvider();
    serializedCredentialsProvider = serialize(credentialsProvider);
    deserializedCredentialsProvider = deserializeCredentialsProvider(serializedCredentialsProvider);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
    serializedCredentialsProvider = serialize(credentialsProvider);
    deserializedCredentialsProvider = deserializeCredentialsProvider(serializedCredentialsProvider);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
  }
}
