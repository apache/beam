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

import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.mockShards;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.kinesis.common.InitialPositionInStream;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KinesisSourceTest {
  @Mock private KinesisClient kinesisClient;
  private KinesisSource source;

  @Test
  public void testSplitGeneratesCorrectNumberOfSources() throws Exception {
    mockShards(kinesisClient, 3);
    source = sourceWithMockedKinesisClient(spec());
    assertThat(source.split(1, opts()).size()).isEqualTo(1);
    assertThat(source.split(2, opts()).size()).isEqualTo(2);
    assertThat(source.split(3, opts()).size()).isEqualTo(3);
    // there are only 3 shards, no more than 3 splits can be created
    assertThat(source.split(4, opts()).size()).isEqualTo(3);
  }

  @Test
  public void shouldHandleExpiredIterationExceptionForShardListing() {
    shouldHandleShardListingError(
        ExpiredIteratorException.builder().build(), ExpiredIteratorException.class);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        LimitExceededException.builder().build(), LimitExceededException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        ProvisionedThroughputExceededException.builder().build(),
        ProvisionedThroughputExceededException.class);
  }

  @Test
  public void shouldHandleServiceErrorForShardListing() {
    shouldHandleShardListingError(
        SdkServiceException.builder().statusCode(HttpStatusCode.GATEWAY_TIMEOUT).build(),
        SdkServiceException.class);
  }

  @Test
  public void shouldHandleRetryableClientErrorForShardListing() {
    shouldHandleShardListingError(
        ApiCallAttemptTimeoutException.builder().build(), ApiCallAttemptTimeoutException.class);
  }

  @Test
  public void shouldHandleUnexpectedExceptionForShardListing() {
    shouldHandleShardListingError(new NullPointerException(), NullPointerException.class);
  }

  private KinesisSource sourceWithMockedKinesisClient(KinesisIO.Read read) {
    return new KinesisSource(read) {
      @Override
      KinesisClient createKinesisClient(KinesisIO.Read spec, PipelineOptions options) {
        return kinesisClient;
      }
    };
  }

  private PipelineOptions opts() {
    AwsOptions options = PipelineOptionsFactory.fromArgs().as(AwsOptions.class);
    options.setAwsRegion(Region.AP_EAST_1);
    return options;
  }

  private KinesisIO.Read spec() {
    return KinesisIO.read()
        .withStreamName("stream")
        .withInitialPositionInStream(InitialPositionInStream.LATEST);
  }

  private void shouldHandleShardListingError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    when(kinesisClient.listShards(any(ListShardsRequest.class))).thenThrow(thrownException);
    try {
      source = sourceWithMockedKinesisClient(spec());
      source.split(1, opts());
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesisClient);
    }
  }
}
