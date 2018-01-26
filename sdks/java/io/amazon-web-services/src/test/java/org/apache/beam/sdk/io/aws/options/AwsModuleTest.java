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

package org.apache.beam.sdk.io.aws.options;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link AwsModule}.
 */
@RunWith(JUnit4.class)
public class AwsModuleTest {

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new AwsModule());

  @Test
  public void testObjectMapperIsAbleToFindModule() throws Exception {
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    assertThat(modules, hasItem(Matchers.instanceOf(AwsModule.class)));
  }

  @Test
  public void testAWSStaticCredentialsProviderSerializationDeserialization() throws Exception {

    String awsKeyId = "key-id";
    String awsSecretKey = "secret-key";

    AWSStaticCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsKeyId, awsSecretKey));

    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    assertEquals(credentialsProvider.getCredentials().getAWSAccessKeyId(),
        deserializedCredentialsProvider.getCredentials().getAWSAccessKeyId());
    assertEquals(credentialsProvider.getCredentials().getAWSSecretKey(),
        deserializedCredentialsProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testPropertiesFileCredentialsProviderSerializationDeserialization() throws Exception {

    String credentialsFilePath = "/path/to/file";

    PropertiesFileCredentialsProvider credentialsProvider =
        new PropertiesFileCredentialsProvider(credentialsFilePath);

    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    Field field =
        PropertiesFileCredentialsProvider.class.getDeclaredField("credentialsFilePath");
    field.setAccessible(true);
    String deserializedCredentialsFilePath = ((String) field.get(deserializedCredentialsProvider));
    assertEquals(credentialsFilePath, deserializedCredentialsFilePath);
  }

  @Test
  public void testClasspathPropertiesFileCredentialsProviderSerializationDeserialization()
      throws Exception {

    String credentialsFilePath = "/path/to/file";

    ClasspathPropertiesFileCredentialsProvider credentialsProvider =
        new ClasspathPropertiesFileCredentialsProvider(credentialsFilePath);

    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    Field field =
        ClasspathPropertiesFileCredentialsProvider.class.getDeclaredField("credentialsFilePath");
    field.setAccessible(true);
    String deserializedCredentialsFilePath = ((String) field.get(deserializedCredentialsProvider));
    assertEquals(credentialsFilePath, deserializedCredentialsFilePath);
  }

  @Test
  public void testSingletonAWSCredentialsProviderSerializationDeserialization() throws Exception {
    AWSCredentialsProvider credentialsProvider;
    String serializedCredentialsProvider;
    AWSCredentialsProvider deserializedCredentialsProvider;

    credentialsProvider = new DefaultAWSCredentialsProviderChain();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new EnvironmentVariableCredentialsProvider();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new SystemPropertiesCredentialsProvider();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new ProfileCredentialsProvider();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());

    credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
    serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);
    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
  }
}
