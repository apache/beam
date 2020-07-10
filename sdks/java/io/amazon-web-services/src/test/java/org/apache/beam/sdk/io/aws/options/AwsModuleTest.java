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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
  public void testAWSStaticCredentialsProviderSerializationDeserialization() throws Exception {
    String awsKeyId = "key-id";
    String awsSecretKey = "secret-key";

    AWSStaticCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsKeyId, awsSecretKey));

    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    assertEquals(
        credentialsProvider.getCredentials().getAWSAccessKeyId(),
        deserializedCredentialsProvider.getCredentials().getAWSAccessKeyId());
    assertEquals(
        credentialsProvider.getCredentials().getAWSSecretKey(),
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

    Field field = PropertiesFileCredentialsProvider.class.getDeclaredField("credentialsFilePath");
    field.setAccessible(true);
    String deserializedCredentialsFilePath = (String) field.get(deserializedCredentialsProvider);
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
    String deserializedCredentialsFilePath = (String) field.get(deserializedCredentialsProvider);
    assertEquals(credentialsFilePath, deserializedCredentialsFilePath);
  }

  @Test
  public void testSTSAssumeRoleSessionCredentialsProviderSerializationDeserialization()
      throws Exception {
    String roleArn = "arn:aws:iam::000111222333:role/TestRole";
    String roleSessionName = "roleSessionName";
    STSAssumeRoleSessionCredentialsProvider credentialsProvider =
        new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName).build();
    String serializedCredentialsProvider = objectMapper.writeValueAsString(credentialsProvider);
    AWSCredentialsProvider deserializedCredentialsProvider =
        objectMapper.readValue(serializedCredentialsProvider, AWSCredentialsProvider.class);

    assertEquals(credentialsProvider.getClass(), deserializedCredentialsProvider.getClass());
    Field fieldRole = STSAssumeRoleSessionCredentialsProvider.class.getDeclaredField("roleArn");
    fieldRole.setAccessible(true);
    String deserializedRoleArn = (String) fieldRole.get(deserializedCredentialsProvider);
    assertEquals(roleArn, deserializedRoleArn);

    Field fieldSession =
        STSAssumeRoleSessionCredentialsProvider.class.getDeclaredField("roleSessionName");
    fieldSession.setAccessible(true);
    String deserializedRoleSessionName = (String) fieldSession.get(deserializedCredentialsProvider);
    assertEquals(roleSessionName, deserializedRoleSessionName);
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

  @Test
  public void testSSECustomerKeySerializationDeserialization() throws Exception {
    final String key = "86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=";
    final String md5 = null;
    final String algorithm = "AES256";

    SSECustomerKey value = new SSECustomerKey(key);

    String valueAsJson = objectMapper.writeValueAsString(value);
    SSECustomerKey valueDes = objectMapper.readValue(valueAsJson, SSECustomerKey.class);
    assertEquals(key, valueDes.getKey());
    assertEquals(algorithm, valueDes.getAlgorithm());
    assertEquals(md5, valueDes.getMd5());
  }

  @Test
  public void testSSEAwsKeyManagementParamsSerializationDeserialization() throws Exception {
    final String awsKmsKeyId =
        "arn:aws:kms:eu-west-1:123456789012:key/dc123456-7890-ABCD-EF01-234567890ABC";
    final String encryption = "aws:kms";
    SSEAwsKeyManagementParams value = new SSEAwsKeyManagementParams(awsKmsKeyId);

    String valueAsJson = objectMapper.writeValueAsString(value);
    SSEAwsKeyManagementParams valueDes =
        objectMapper.readValue(valueAsJson, SSEAwsKeyManagementParams.class);
    assertEquals(awsKmsKeyId, valueDes.getAwsKmsKeyId());
    assertEquals(encryption, valueDes.getEncryption());
  }

  @Test
  public void testClientConfigurationSerializationDeserialization() throws Exception {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setProxyHost("localhost");
    clientConfiguration.setProxyPort(1234);
    clientConfiguration.setProxyUsername("username");
    clientConfiguration.setProxyPassword("password");

    final String valueAsJson = objectMapper.writeValueAsString(clientConfiguration);
    final ClientConfiguration valueDes =
        objectMapper.readValue(valueAsJson, ClientConfiguration.class);
    assertEquals("localhost", valueDes.getProxyHost());
    assertEquals(1234, valueDes.getProxyPort());
    assertEquals("username", valueDes.getProxyUsername());
    assertEquals("password", valueDes.getProxyPassword());
  }

  @Test
  public void testAwsHttpClientConfigurationSerializationDeserialization() throws Exception {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setConnectionTimeout(100);
    clientConfiguration.setConnectionMaxIdleMillis(1000);
    clientConfiguration.setSocketTimeout(300);

    final String valueAsJson = objectMapper.writeValueAsString(clientConfiguration);
    final ClientConfiguration clientConfigurationDeserialized =
        objectMapper.readValue(valueAsJson, ClientConfiguration.class);
    assertEquals(100, clientConfigurationDeserialized.getConnectionTimeout());
    assertEquals(1000, clientConfigurationDeserialized.getConnectionMaxIdleMillis());
    assertEquals(300, clientConfigurationDeserialized.getSocketTimeout());
  }
}
