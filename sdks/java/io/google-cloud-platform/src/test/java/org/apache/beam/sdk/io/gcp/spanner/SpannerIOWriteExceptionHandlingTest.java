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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerExceptionFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationMode;

@RunWith(Parameterized.class)
public class SpannerIOWriteExceptionHandlingTest {

  private static final long CELLS_PER_KEY = 7;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private FakeServiceFactory serviceFactory;

  @Captor public transient ArgumentCaptor<Iterable<Mutation>> mutationBatchesCaptor;
  @Captor public transient ArgumentCaptor<Options.ReadQueryUpdateTransactionOption> optionsCaptor;

  // Using
  // https://cloud.google.com/java/docs/reference/google-cloud-spanner/latest/com.google.cloud.spanner.ErrorCode
  // to select test cases and make sure that we're dealing with them appropriately.
  // The main goal of these tests is to make sure that no exception is ever swallowed.
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // DEADLINE_EXCEEDED is the only exception type that generates retries in-SDK
          // The default backoff generates at least 8 retries, but can sometimes generate as many as
          // 10 retries due to randomness in the backoff calculation. As this randomness is not
          // easily controlled in testing we use an "atLeast" rather than "exactly" test.
          {ErrorCode.DEADLINE_EXCEEDED, "deadline passed!", atLeast(8), atLeast(9)},

          // All other error codes do not generate in-SDK retries, and the errors are thrown out.
          {ErrorCode.ABORTED, "transaction aborted!", times(0), times(1)},
          {ErrorCode.PERMISSION_DENIED, "permission denied, buddy!", times(0), times(1)},
          {ErrorCode.INTERNAL, "internal error. idk!", times(0), times(1)},
          {ErrorCode.RESOURCE_EXHAUSTED, "resource exhausted very tired!", times(0), times(1)},
          {ErrorCode.UNAUTHENTICATED, "authenticate!", times(0), times(1)},
          {ErrorCode.NOT_FOUND, "not found the thing", times(0), times(1)},
          {ErrorCode.FAILED_PRECONDITION, "conditions prestart are failed", times(0), times(1)},
        });
  }

  private final ErrorCode exceptionErrorcode;
  private final String errorString;
  private final VerificationMode callsToSleeperVerification;
  private final VerificationMode callsToWriteVerification;

  public SpannerIOWriteExceptionHandlingTest(
      ErrorCode exceptionErrorcode,
      String errorString,
      VerificationMode callsToSleeperVerification,
      VerificationMode callsToWriteVerification) {
    this.exceptionErrorcode = exceptionErrorcode;
    this.errorString = errorString;
    this.callsToSleeperVerification = callsToSleeperVerification;
    this.callsToWriteVerification = callsToWriteVerification;
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    serviceFactory = new FakeServiceFactory();

    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Capture batches sent to writeAtLeastOnceWithOptions.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(mutationBatchesCaptor.capture(), optionsCaptor.capture()))
        .thenReturn(null);

    // Simplest schema: a table with int64 key
    SpannerIOWriteTest.preparePkMetadata(
        tx, Arrays.asList(SpannerIOWriteTest.pkMetadata("tEsT", "key", "ASC")));
    SpannerIOWriteTest.prepareColumnMetadata(
        tx,
        Arrays.asList(SpannerIOWriteTest.columnMetadata("tEsT", "key", "INT64", CELLS_PER_KEY)));
    SpannerIOWriteTest.preparePgColumnMetadata(
        tx,
        Arrays.asList(SpannerIOWriteTest.columnMetadata("tEsT", "key", "bigint", CELLS_PER_KEY)));

    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
  }

  @Test
  public void testExceptionHandlingForSimpleWrite() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(SpannerIOWriteTest.buildUpsertMutation((long) 1));

    // mock sleeper so that it does not actually sleep.
    SpannerIO.WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(
                any(), any(Options.ReadQueryUpdateTransactionOption.class)))
        .thenThrow(SpannerExceptionFactory.newSpannerException(exceptionErrorcode, errorString));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage(errorString);

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(SpannerIO.FailureMode.FAIL_FAST));

    // One error
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(1, Iterables.size(m));
              return null;
            });
    try {
      pipeline.run().waitUntilFinish();
    } finally {
      verify(SpannerIO.WriteToSpannerFn.sleeper, callsToSleeperVerification).sleep(anyLong());
      verify(serviceFactory.mockDatabaseClient(), callsToWriteVerification)
          .writeAtLeastOnceWithOptions(any(), any(Options.ReadQueryUpdateTransactionOption.class));
    }
  }

  @Test
  public void testExceptionHandlingForWriteGrouped() throws InterruptedException {
    List<MutationGroup> mutationList =
        Arrays.asList(
            SpannerIOWriteTest.buildMutationGroup(
                SpannerIOWriteTest.buildUpsertMutation((long) 1)));

    // mock sleeper so that it does not actually sleep.
    SpannerIO.WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(
                any(), any(Options.ReadQueryUpdateTransactionOption.class)))
        .thenThrow(SpannerExceptionFactory.newSpannerException(exceptionErrorcode, errorString));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage(errorString);

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(SpannerIO.FailureMode.FAIL_FAST)
                    .grouped());

    // Zero error
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(0, Iterables.size(m));
              return null;
            });
    try {
      pipeline.run().waitUntilFinish();
    } finally {
      verify(SpannerIO.WriteToSpannerFn.sleeper, callsToSleeperVerification).sleep(anyLong());
      verify(serviceFactory.mockDatabaseClient(), callsToWriteVerification)
          .writeAtLeastOnceWithOptions(any(), any(Options.ReadQueryUpdateTransactionOption.class));
    }
  }
}
