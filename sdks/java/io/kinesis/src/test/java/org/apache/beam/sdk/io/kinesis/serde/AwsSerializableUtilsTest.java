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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
    String serializedProvider = AwsSerializableUtils.serialize(credentialsProvider);

    checkStaticBasicCredentials(AwsSerializableUtils.deserialize(serializedProvider));
  }

  @Test
  public void testStaticSessionCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(
            new BasicSessionCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN));
    String serializedCredentials = AwsSerializableUtils.serialize(credentialsProvider);

    checkStaticSessionCredentials(AwsSerializableUtils.deserialize(serializedCredentials));
  }

  @Test
  public void testDefaultCredentialsProviderSerialization() {
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
    String serializedCredentials = AwsSerializableUtils.serialize(credentialsProvider);

    assertEquals(credentialsProvider, AwsSerializableUtils.deserialize(serializedCredentials));
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
    AwsSerializableUtils.serialize(credentialsProvider);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnAWSCredentialsProviderDeserialization() {
    AwsSerializableUtils.deserialize("invalid string");
  }
}
