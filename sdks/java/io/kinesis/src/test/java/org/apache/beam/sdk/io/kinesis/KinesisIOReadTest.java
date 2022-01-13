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
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import org.apache.beam.sdk.io.kinesis.KinesisIO.Read;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for non trivial builder variants of {@link KinesisIO#read}. */
@RunWith(JUnit4.class)
public class KinesisIOReadTest {
  private static final String ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String SECRET_ACCESS_KEY = "SECRET_ACCESS_KEY";
  private static final boolean VERIFICATION_DISABLED = false;

  @Test
  public void testReadWithBasicCredentials() {
    Regions region = Regions.US_EAST_1;
    Read<KinesisRecord> read =
        KinesisIO.read().withAWSClientsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY, region);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY)),
            region,
            null,
            true));
  }

  @Test
  public void testReadWithCredentialsProvider() {
    Regions region = Regions.US_EAST_1;
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    Read<KinesisRecord> read = KinesisIO.read().withAWSClientsProvider(credentialsProvider, region);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(credentialsProvider, region, null, true));
  }

  @Test
  public void testReadWithBasicCredentialsAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Regions region = Regions.US_WEST_1;

    Read<KinesisRecord> read =
        KinesisIO.read()
            .withAWSClientsProvider(ACCESS_KEY_ID, SECRET_ACCESS_KEY, region, customEndpoint);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY)),
            region,
            customEndpoint,
            true));
  }

  @Test
  public void testReadWithCredentialsProviderAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Regions region = Regions.US_WEST_1;
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    Read<KinesisRecord> read =
        KinesisIO.read().withAWSClientsProvider(credentialsProvider, region, customEndpoint);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(credentialsProvider, region, customEndpoint, true));
  }

  @Test
  public void testReadWithBasicCredentialsAndVerificationDisabled() {
    String customEndpoint = "localhost:9999";
    Regions region = Regions.US_WEST_1;

    Read<KinesisRecord> read =
        KinesisIO.read()
            .withAWSClientsProvider(
                ACCESS_KEY_ID, SECRET_ACCESS_KEY, region, customEndpoint, VERIFICATION_DISABLED);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY)),
            region,
            customEndpoint,
            VERIFICATION_DISABLED));
  }

  @Test
  public void testReadWithCredentialsProviderAndVerificationDisabled() {
    String customEndpoint = "localhost:9999";
    Regions region = Regions.US_WEST_1;
    AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    Read<KinesisRecord> read =
        KinesisIO.read()
            .withAWSClientsProvider(
                credentialsProvider, region, customEndpoint, VERIFICATION_DISABLED);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(
            credentialsProvider, region, customEndpoint, VERIFICATION_DISABLED));
  }
}
