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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/** Tests for non trivial builder variants of {@link KinesisIO#read}. */
@RunWith(JUnit4.class)
public class KinesisIOReadTest {

  @Test
  public void testBuildWithBasicCredentials() {
    Region region = Region.US_EAST_1;
    AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");

    Read read =
        KinesisIO.read()
            .withAWSClientsProvider(
                credentials.accessKeyId(), credentials.secretAccessKey(), region);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(StaticCredentialsProvider.create(credentials), region, null));
  }

  @Test
  public void testBuildWithCredentialsProvider() {
    Region region = Region.US_EAST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    Read read = KinesisIO.read().withAWSClientsProvider(credentialsProvider, region);

    assertEquals(
        read.getAWSClientsProvider(), new BasicKinesisProvider(credentialsProvider, region, null));
  }

  @Test
  public void testBuildWithBasicCredentialsAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Region region = Region.US_WEST_1;
    AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");

    Read read =
        KinesisIO.read()
            .withAWSClientsProvider(
                credentials.accessKeyId(), credentials.secretAccessKey(), region, customEndpoint);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(
            StaticCredentialsProvider.create(credentials), region, customEndpoint));
  }

  @Test
  public void testBuildWithCredentialsProviderAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Region region = Region.US_WEST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    Read read =
        KinesisIO.read().withAWSClientsProvider(credentialsProvider, region, customEndpoint);

    assertEquals(
        read.getAWSClientsProvider(),
        new BasicKinesisProvider(credentialsProvider, region, customEndpoint));
  }
}
