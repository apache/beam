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
package org.apache.beam.sdk.io.aws2.dynamodb;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** Tests on {@link BasicDynamoDbClientProvider}. */
@RunWith(JUnit4.class)
public class BasicDynamoDbClientProviderTest {

  @Test
  public void testSerialization() {
    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create("ACCESS_KEY_ID", "SECRET_ACCESS_KEY"));

    BasicDynamoDbClientProvider dynamoDbClientProvider =
        new BasicDynamoDbClientProvider(awsCredentialsProvider, "us-east-1", null);

    byte[] serializedBytes = SerializableUtils.serializeToByteArray(dynamoDbClientProvider);

    BasicDynamoDbClientProvider dynamoDbClientProviderDeserialized =
        (BasicDynamoDbClientProvider)
            SerializableUtils.deserializeFromByteArray(serializedBytes, "Aws Credentials Provider");

    assertEquals(dynamoDbClientProvider, dynamoDbClientProviderDeserialized);
  }
}
