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

import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Batches together and writes mutations to Google Cloud Spanner. */
@VisibleForTesting
class SpannerWriteGroupFn extends DoFn<MutationGroup, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteGroupFn.class);
  private final SpannerIO.Write spec;
  // Current batch of mutations to be written.
  private List<MutationGroup> mutations;
  private long batchSizeBytes = 0;

  private static final int MAX_RETRIES = 5;
  private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
      FluentBackoff.DEFAULT
          .withMaxRetries(MAX_RETRIES)
          .withInitialBackoff(Duration.standardSeconds(5));

  private transient SpannerAccessor spannerAccessor;

  @VisibleForTesting
  SpannerWriteGroupFn(SpannerIO.Write spec) {
    this.spec = spec;
  }

  @Setup
  public void setup() throws Exception {
    spannerAccessor = spec.getSpannerConfig().connectToSpanner();
    mutations = new ArrayList<>();
    batchSizeBytes = 0;
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    MutationGroup m = c.element();
    mutations.add(m);
    batchSizeBytes += MutationSizeEstimator.sizeOf(m);
    if (batchSizeBytes >= spec.getBatchSizeBytes()) {
      flushBatch();
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    if (!mutations.isEmpty()) {
      flushBatch();
    }
  }

  /**
   * Writes a batch of mutations to Cloud Spanner.
   *
   * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times. If the retry limit
   * is exceeded, the last exception from Cloud Spanner will be thrown.
   *
   * @throws AbortedException if the commit fails or IOException or InterruptedException if
   *     backing off between retries fails.
   */
  private void flushBatch() throws AbortedException, IOException, InterruptedException {
    LOG.debug("Writing batch of {} mutations", mutations.size());
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    while (true) {
      // Batch upsert rows.
      try {
        databaseClient.writeAtLeastOnce(Iterables.concat(mutations));

        // Break if the commit threw no exception.
        break;
      } catch (AbortedException exception) {
        // Only log the code and message for potentially-transient errors. The entire exception
        // will be propagated upon the last retry.
        LOG.error(
            "Error writing to Spanner ({}): {}", exception.getCode(), exception.getMessage());
        if (!BackOffUtils.next(sleeper, backoff)) {
          LOG.error("Aborting after {} retries.", MAX_RETRIES);
          throw exception;
        }
      }
    }
    LOG.debug("Successfully wrote {} mutations", mutations.size());
    mutations = new ArrayList<>();
    batchSizeBytes = 0;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    spec.populateDisplayData(builder);
  }
}
