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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.Mutation;
import com.google.spanner.v1.CommitRequest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataSpannerConfigFactoryTest {
  private static final String SPANNER_HOST = "my-host";
  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";

  private MockSpannerServiceImpl mockSpannerService;
  private MockServiceHelper serviceHelper;

  @Before
  public void setUp() throws Exception {
    mockSpannerService = new MockSpannerServiceImpl();
    serviceHelper =
        new MockServiceHelper(SPANNER_HOST, Collections.singletonList(mockSpannerService));
    serviceHelper.start();
    serviceHelper.reset();
  }

  @After
  public void tearDown() {
    serviceHelper.reset();
    serviceHelper.stop();
    mockSpannerService.reset();
  }

  @Test
  public void testAbortedCommitReplaysTransaction() {
    // SpannerIO.readChangeStream() adds ABORTED to the primary config for its streaming queries.
    SpannerConfig primaryConfig =
        SpannerConfig.create()
            .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
            .withIsLocalChannelProvider(StaticValueProvider.of(true))
            .withProjectId(TEST_PROJECT)
            .withInstanceId(TEST_INSTANCE)
            .withDatabaseId(TEST_DATABASE)
            .withRetryableCodes(ImmutableSet.of(Code.UNAVAILABLE, Code.ABORTED));
    SpannerAccessor accessor =
        SpannerAccessor.getOrCreate(
            MetadataSpannerConfigFactory.create(primaryConfig, TEST_INSTANCE, TEST_DATABASE));
    try {
      mockSpannerService.abortNextTransaction();
      AtomicInteger transactionAttempts = new AtomicInteger();
      accessor
          .getDatabaseClient()
          .readWriteTransaction()
          .run(
              transaction -> {
                transactionAttempts.incrementAndGet();
                transaction.buffer(Mutation.delete("metadata", Key.of("token")));
                return null;
              });

      assertEquals(2, transactionAttempts.get());
      assertEquals(2, mockSpannerService.countRequestsOfType(CommitRequest.class));
    } finally {
      accessor.close();
    }
  }
}
