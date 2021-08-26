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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** Tests on {@link AwsSerializableUtils}. */
public class AwsSerializableUtilsTest {

  private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";

  @Test
  public void testAwsCredentialsProviderSerialization() {
    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY));

    String awsCredentialsProviderSerialized =
        AwsSerializableUtils.serializeAwsCredentialsProvider(awsCredentialsProvider);

    AwsCredentialsProvider awsCredentialsProviderDeserialized =
        AwsSerializableUtils.deserializeAwsCredentialsProvider(awsCredentialsProviderSerialized);

    assertTrue(awsCredentialsProviderDeserialized instanceof StaticCredentialsProvider);
    AwsCredentials awsCredentials = awsCredentialsProviderDeserialized.resolveCredentials();
    assertEquals(ACCESS_KEY_ID, awsCredentials.accessKeyId());
    assertEquals(SECRET_ACCESS_KEY, awsCredentials.secretAccessKey());
  }

  static class UnknownAwsCredentialsProvider implements AwsCredentialsProvider {
    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnAwsCredentialsProviderSerialization() {
    AwsCredentialsProvider awsCredentialsProvider = new UnknownAwsCredentialsProvider();
    AwsSerializableUtils.serializeAwsCredentialsProvider(awsCredentialsProvider);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailOnAwsCredentialsProviderDeserialization() {
    AwsSerializableUtils.deserializeAwsCredentialsProvider("invalid string");
  }
}
