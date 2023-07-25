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
package org.apache.beam.sdk.io.kinesis;

import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests on {@link org.apache.beam.sdk.io.aws2.kinesis.BasicKinesisProvider}. */
@RunWith(JUnit4.class)
public class BasicKinesisClientProviderTest {
  private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";

  @Test
  public void testSerialization() {
    AWSCredentialsProvider awsCredentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));

    BasicKinesisProvider kinesisProvider =
        new BasicKinesisProvider(awsCredentialsProvider, Regions.AP_EAST_1, null, true);

    byte[] serializedBytes = SerializableUtils.serializeToByteArray(kinesisProvider);

    BasicKinesisProvider kinesisProviderDeserialized =
        (BasicKinesisProvider)
            SerializableUtils.deserializeFromByteArray(serializedBytes, "Basic Kinesis Provider");

    assertEquals(kinesisProvider, kinesisProviderDeserialized);
  }
}
