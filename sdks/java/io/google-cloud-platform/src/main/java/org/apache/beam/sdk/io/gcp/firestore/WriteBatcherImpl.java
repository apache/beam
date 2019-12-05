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

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;

/**
 * Determines batch sizes for commit RPCs based on past performance.
 * <p>
 * It aims for a target response time per RPC: it uses the response times for previous RPCs and
 * the number of entities contained in them, calculates a rolling average time-per-entity, and
 * chooses the number of entities for future writes to hit the target time.
 * <p>
 * This enables us to send large batches without sending over-large requests in the case of
 * expensive entity writes that may timeout before the server can apply them all.
 * <p>
 * Learn more in [Google SRE Guide](https://landing.google.com/sre/sre-book/chapters/handling-overload/).
 */
@VisibleForTesting
public class WriteBatcherImpl implements WriteBatcher, Serializable {
    /**
     * Target time per RPC for writes.
     */
    static final int FIRESTORE_BATCH_TARGET_LATENCY_MS = 5000;

    /**
     * Starting batch size per RPC for writes.
     */
    @VisibleForTesting
    static final int FIRESTORE_BATCH_UPDATE_ENTITIES_START = 200;

    /**
     * Maximum batch size per RPC for writes.
     */
    @VisibleForTesting
    static final int FIRESTORE_BATCH_UPDATE_ENTITIES_LIMIT = 500;

    /**
     * Minimum batch size per RPC for writes.
     */
    @VisibleForTesting
    static final int FIRESTORE_BATCH_UPDATE_ENTITIES_MIN = 10;

    @Override
    public void start() {
        meanLatencyPerEntityMs = new MovingAverage(
                120000 /* sample period 2 minutes */,
                10000 /* sample interval 10s */,
                1 /* numSignificantBuckets */,
                1 /* numSignificantSamples */);
    }

    @Override
    public void addRequestLatency(long timeSinceEpochMillis, long latencyMillis, int numMutations) {
        meanLatencyPerEntityMs.add(timeSinceEpochMillis, latencyMillis / numMutations);
    }

    @Override
    public int nextBatchSize(long timeSinceEpochMillis) {
        if (!meanLatencyPerEntityMs.hasValue(timeSinceEpochMillis)) {
            return FIRESTORE_BATCH_UPDATE_ENTITIES_START;
        }
        long recentMeanLatency = Math.max(meanLatencyPerEntityMs.get(timeSinceEpochMillis), 1);
        return (int)
                Math.max(
                        FIRESTORE_BATCH_UPDATE_ENTITIES_MIN,
                        Math.min(
                                FIRESTORE_BATCH_UPDATE_ENTITIES_LIMIT,
                                FIRESTORE_BATCH_TARGET_LATENCY_MS / recentMeanLatency));
    }

    private transient MovingAverage meanLatencyPerEntityMs;
}