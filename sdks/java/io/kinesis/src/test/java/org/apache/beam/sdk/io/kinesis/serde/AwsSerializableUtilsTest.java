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

import static org.apache.beam.sdk.io.kinesis.serde.AwsSerializableUtils.deserialize;
import static org.apache.beam.sdk.io.kinesis.serde.AwsSerializableUtils.serialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSCredentials;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AwsSerializableUtilsTest {

  private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";
  private static final String SESSION_TOKEN = "SESSION_TOKEN";

  private void checkStaticBasicCredentials(AWSCredentialsProvider provider) {
    assertTrue(provider instanceof AWSStaticCredentialsProvider);
    assertEquals(ACCESS_KEY_ID, provider.getCredentials().getAWSAccessKeyId());
    assertEquals(SECRET_ACCESS_KEY, provider.getCredentials().getAWSSecretKey());
  }

  private void checkStaticSessionCredentials(AWSCredentialsProvider provider) {
    checkStaticBasicCredentials(provider);
    assertEquals(
        SESSION_TOKEN, ((AWSSessionCredentials) provider.getCredentials()).getSessionToken());
  }

  @Test
  public void testBasicCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
    String serializedProvider = serialize(credentialsProvider);

    checkStaticBasicCredentials(deserialize(serializedProvider));
  }

  @Test
  public void testStaticSessionCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN));
    String serializedCredentials = serialize(credentialsProvider);

    checkStaticSessionCredentials(deserialize(serializedCredentials));
  }

  @Test
  public void testDefaultAWSCredentialsProviderChainSerialization() {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    String expectedSerializedProvider = "{\"@type\":\"DefaultAWSCredentialsProviderChain\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testPropertiesFileCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider =
        new PropertiesFileCredentialsProvider("AwsCredentials.properties");
    String expectedSerializedProvider =
        "{\"@type\":\"PropertiesFileCredentialsProvider\",\"credentialsFilePath\":\"AwsCredentials.properties\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testClasspathPropertiesFileCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider =
        new ClasspathPropertiesFileCredentialsProvider("AwsCredentials.properties");
    String expectedSerializedProvider =
        "{\"@type\":\"ClasspathPropertiesFileCredentialsProvider\",\"credentialsFilePath\":\"/AwsCredentials.properties\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testEnvironmentVariableCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
    String expectedSerializedProvider = "{\"@type\":\"EnvironmentVariableCredentialsProvider\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testSystemPropertiesCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider = new SystemPropertiesCredentialsProvider();
    String expectedSerializedProvider = "{\"@type\":\"SystemPropertiesCredentialsProvider\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testProfileCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    String expectedSerializedProvider = "{\"@type\":\"ProfileCredentialsProvider\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  @Test
  public void testEC2ContainerCredentialsProviderWrapperSerialization() {
    AWSCredentialsProvider credentialsProvider = new EC2ContainerCredentialsProviderWrapper();
    String expectedSerializedProvider = "{\"@type\":\"EC2ContainerCredentialsProviderWrapper\"}";
    String serializedProvider = serialize(credentialsProvider);

    assertEquals(expectedSerializedProvider, serializedProvider);
    assertEquals(expectedSerializedProvider, serialize(deserialize(serializedProvider)));
  }

  static class UnknownAwsCredentialsProvider implements AWSCredentialsProvider {
    @Override
    public AWSCredentials getCredentials() {
      return new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    }

    @Override
    public void refresh() {}
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnAWSCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider = new UnknownAwsCredentialsProvider();
    serialize(credentialsProvider);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnAWSCredentialsProviderDeserialization() {
    deserialize("invalid string");
  }
}
