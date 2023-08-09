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

import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createIOOptions;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.mockShards;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.kinesis.common.InitialPositionInStream;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KinesisSourceTest {
  @Mock private KinesisClient kinesisClient;
  private PipelineOptions options = createOptions();

  @Test
  public void testCreateReaderOfCorrectType() throws Exception {
    KinesisIO.Read readSpec =
        KinesisIO.read()
            .withStreamName("stream-xxx")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    KinesisIO.Read readSpecEFO =
        KinesisIO.read()
            .withStreamName("stream-xxx")
            .withConsumerArn("consumer-aaa")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    KinesisReaderCheckpoint initCheckpoint = new KinesisReaderCheckpoint(ImmutableList.of());

    UnboundedSource.UnboundedReader<KinesisRecord> reader =
        new KinesisSource(readSpec, initCheckpoint).createReader(options, null);
    assertThat(reader).isInstanceOf(KinesisReader.class);

    UnboundedSource.UnboundedReader<KinesisRecord> efoReader =
        new KinesisSource(readSpecEFO, initCheckpoint).createReader(options, null);
    assertThat(efoReader).isInstanceOf(EFOKinesisReader.class);
  }

  @Test
  public void testSplitGeneratesCorrectNumberOfSources() throws Exception {
    mockShards(kinesisClient, 3);
    KinesisSource source = sourceWithMockedKinesisClient(spec());
    assertThat(source.split(1, options).size()).isEqualTo(1);
    assertThat(source.split(2, options).size()).isEqualTo(2);
    assertThat(source.split(3, options).size()).isEqualTo(3);
    // there are only 3 shards, no more than 3 splits can be created
    assertThat(source.split(4, options).size()).isEqualTo(3);
  }

  @Test
  public void shouldThrowLimitExceededExceptionForShardListing() {
    shouldThrowShardListingError(
        LimitExceededException.builder().build(), LimitExceededException.class);
  }

  @Test
  public void shouldThrowServiceErrorForShardListing() {
    shouldThrowShardListingError(
        SdkServiceException.builder().statusCode(HttpStatusCode.GATEWAY_TIMEOUT).build(),
        SdkServiceException.class);
  }

  private KinesisSource sourceWithMockedKinesisClient(KinesisIO.Read read) {
    MockClientBuilderFactory.set(options, KinesisClientBuilder.class, kinesisClient);
    return new KinesisSource(read);
  }

  private PipelineOptions createOptions() {
    AwsOptions options = PipelineOptionsFactory.fromArgs().as(AwsOptions.class);
    options.setAwsRegion(Region.AP_EAST_1);
    return options;
  }

  private KinesisIO.Read spec() {
    return KinesisIO.read()
        .withStreamName("stream")
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  private void shouldThrowShardListingError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    when(kinesisClient.listShards(any(ListShardsRequest.class))).thenThrow(thrownException);
    try {
      KinesisSource source = sourceWithMockedKinesisClient(spec());
      source.split(1, options);
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesisClient);
    }
  }

  @Test
  public void testConsumerArnNotPassed() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");
    KinesisIOOptions options = createIOOptions();
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isNull();
  }

  @Test
  public void testConsumerArnPassedInIO() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn::consumer-yyy");

    KinesisIOOptions options = createIOOptions();
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isEqualTo("arn::consumer-yyy");
  }

  @Test
  public void testConsumerArnPassedInPipelineOptions() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");

    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": \"arn-01\"}");
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isEqualTo("arn-01");
  }

  @Test
  public void testConsumerArnForSpecificStreamNotPassedInPipelineOptions() {
    KinesisIO.Read readSpec = KinesisIO.read().withStreamName("stream-xxx");
    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-01\": \"arn-01\"}");
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isNull();
  }

  @Test
  public void testConsumerArnInPipelineOptionsOverwritesIOSetting() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn-ignored");

    KinesisIOOptions options =
        createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": \"arn-01\"}");
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isEqualTo("arn-01");
  }

  @Test
  public void testConsumerArnInPipelineOptionsDiscardsIOSetting() {
    KinesisIO.Read readSpec =
        KinesisIO.read().withStreamName("stream-xxx").withConsumerArn("arn-ignored");

    KinesisIOOptions options = createIOOptions("--kinesisIOConsumerArns={\"stream-xxx\": null}");
    assertThat(KinesisSource.resolveConsumerArn(readSpec, options)).isNull();
  }
}
