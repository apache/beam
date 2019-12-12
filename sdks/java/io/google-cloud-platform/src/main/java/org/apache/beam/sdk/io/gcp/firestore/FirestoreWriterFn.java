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

package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreException;
import com.google.cloud.firestore.FirestoreFactory;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.rpc.Code;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class FirestoreWriterFn<T> extends DoFn<T, Void> {

    private static final Set<Integer> NON_RETRYABLE_ERRORS =
            ImmutableSet.of(
                    Code.FAILED_PRECONDITION.getNumber(),
                    Code.INVALID_ARGUMENT.getNumber(),
                    Code.PERMISSION_DENIED.getNumber(),
                    Code.UNAUTHENTICATED.getNumber());
    private static final Logger LOG = LoggerFactory.getLogger(FirestoreWriterFn.class);

    // Current batch of mutations to be written.
    private final List<T> mutations = new ArrayList<T>();
    private int mutationsSize = 0; // Accumulated size of protos in mutations.
    private WriteBatcher writeBatcher;
    private transient AdaptiveThrottler throttler;
    private final Counter throttledSeconds =
            Metrics.counter(FirestoreWriterFn.class, "cumulativeThrottlingSeconds");
    private final Counter rpcErrors =
            Metrics.counter(FirestoreWriterFn.class, "firestoreRpcErrors");
    private final Counter rpcSuccesses =
            Metrics.counter(FirestoreWriterFn.class, "firestoreRpcSuccesses");

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
            FluentBackoff.DEFAULT
                    .withMaxRetries(MAX_RETRIES)
                    .withInitialBackoff(Duration.standardSeconds(5));
    private String projectId;
    private String collectionId;
    private Firestore firestore;
    private String documentId;
    private FirestoreFactory firestoreFactory;
    private BatchRequesterFactory<T> batchRequesterFactory;

    public FirestoreWriterFn(String projectId, String collectionId, String documentId, FirestoreFactory firestoreFactory, WriteBatcher writeBatcher, BatchRequesterFactory<T> batchRequesterFactory) {
        this.projectId = projectId;
        this.collectionId = collectionId;
        this.firestoreFactory = firestoreFactory;
        this.writeBatcher = writeBatcher;
        this.documentId = documentId;
        this.batchRequesterFactory = batchRequesterFactory;
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
        if (projectId == null) {
            firestore = firestoreFactory.create(FirestoreOptions.getDefaultInstance());
        } else {
            firestore = firestoreFactory.create(FirestoreOptions.newBuilder().setProjectId(projectId).setDatabaseId("default").build());
        }
        writeBatcher.start();
        if (throttler == null) {
            // Initialize throttler at first use, because it is not serializable.
            throttler = new AdaptiveThrottler(120000, 10000, 1.25);
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        mutations.add(c.element());
        if (mutations.size() >= writeBatcher.nextBatchSize(System.currentTimeMillis())) {
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
     * Writes a batch of mutations to Cloud Firestore.
     * <p>
     * A batch of writes completes atomically and can write to multiple documents.
     * If a commit fails, all mutations in the batch will fail and it will be retried up to {@link #MAX_RETRIES} times.
     * If the retry limit is exceeded, the last exception from Cloud Firestore will be thrown.
     * <p>
     * Learn more in [Firestore docs](https://cloud.google.com/firestore/docs/manage-data/transactions#batched-writes).
     *
     * @throws FirestoreException if the commit fails or IOException or InterruptedException if backing off between retries fails.
     */
    private void flushBatch() throws FirestoreException, IOException, InterruptedException, ExecutionException {
        LOG.debug("Writing batch of {} mutations", mutations.size());
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

        while (true) {

            long startTime = System.currentTimeMillis(), endTime;

            if (throttler.throttleRequest(startTime)) {
                LOG.info("Delaying request due to previous failures");
                throttledSeconds.inc(WriteBatcherImpl.FIRESTORE_BATCH_TARGET_LATENCY_MS / 1000);
                sleeper.sleep(WriteBatcherImpl.FIRESTORE_BATCH_TARGET_LATENCY_MS);
                continue;
            }

            try {
                FirestoreBatchRequester<T> firestoreBatchRequester = batchRequesterFactory.create(firestore);
                firestoreBatchRequester.commit(mutations, collectionId, documentId);

                endTime = System.currentTimeMillis();

                writeBatcher.addRequestLatency(endTime, endTime - startTime, mutations.size());
                throttler.successfulRequest(startTime);
                rpcSuccesses.inc();

                // Break if the commit threw no exception.
                break;
            } catch (FirestoreException exception) {
                if (exception.getCode() == Code.DEADLINE_EXCEEDED.getNumber()) {
                    /* Most errors are not related to request size, and should not change our expectation of
                     * the latency of successful requests. DEADLINE_EXCEEDED can be taken into
                     * consideration, though. */
                    endTime = System.currentTimeMillis();
                    writeBatcher.addRequestLatency(endTime, endTime - startTime, mutations.size());
                }
                // Only log the code and message for potentially-transient errors. The entire exception
                // will be propagated upon the last retry.
                LOG.error(
                        "Error writing batch of {} mutations to Firestore with key {} ({}): {}",
                        mutations.size(),
                        documentId,
                        exception.getCode(),
                        exception.getMessage());
                rpcErrors.inc();

                if (NON_RETRYABLE_ERRORS.contains(exception.getCode())) {
                    throw exception;
                }
                if (!BackOffUtils.next(sleeper, backoff)) {
                    LOG.error("Aborting after {} retries.", MAX_RETRIES);
                    throw exception;
                }
            } catch (ExecutionException exception) {
                endTime = System.currentTimeMillis();
                writeBatcher.addRequestLatency(endTime, endTime - startTime, mutations.size());
                LOG.error(
                        "Error writing batch of {} mutations to Firestore: {}",
                        mutations.size(),
                        exception.getMessage());
                rpcErrors.inc();

                if (!BackOffUtils.next(sleeper, backoff)) {
                    LOG.error("Aborting after {} retries.", MAX_RETRIES);
                    throw exception;
                }
            }
            LOG.debug("Successfully wrote {} mutations", mutations.size());
            mutations.clear();
            mutationsSize = 0;
        }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.addIfNotNull(DisplayData.item("projectId", projectId).withLabel("Output Project"))
                .addIfNotNull(DisplayData.item("collectionId", collectionId).withLabel("Output Collection"))
                .addIfNotNull(DisplayData.item("documentId", documentId).withLabel("Output Document"));
    }
}