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
package org.apache.beam.sdk.io.iceberg;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that batches elements for a SINGLE EXPECTED KEY using static variables, aggregating
 * across bundles within the same worker JVM.
 *
 * <p>WARNING: This approach is NOT fault-tolerant. Data buffered in static variables will be lost
 * if the worker crashes. Intended for performance testing only. Batches may also be stranded if the
 * process ends. THIS VERSION ASSUMES ONLY ONE KEY EVER EXISTS.
 */
class LocalGroupIntoBatches
    extends PTransform<
        PCollection<KV<String, Row>>, PCollection<KV<ShardedKey<String>, Iterable<Row>>>> {

  private final int maxBatchSize;

  public LocalGroupIntoBatches(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  @Override
  public PCollection<KV<ShardedKey<String>, Iterable<Row>>> expand(
      PCollection<KV<String, Row>> input) {
    return input.apply(
        "StaticVarBatchingDoFnSingleKey",
        ParDo.of(new StaticVarBatchingDoFnSingleKey(maxBatchSize)));
  }

  private static class StaticVarBatchingDoFnSingleKey
      extends DoFn<KV<String, Row>, KV<ShardedKey<String>, Iterable<Row>>> {
    private static final Logger LOG = LoggerFactory.getLogger(StaticVarBatchingDoFnSingleKey.class);
    static final byte[] EMPTY_SHARD_VALUE = new byte[0];

    // Shared state for the single key
    private static final List<Row> staticBatch = new ArrayList<>();
    private static final Object staticBatchLock = new Object();
    private static volatile @Nullable String globalBatchKey = null;

    private final int maxBatchSize;

    // Transient state per DoFn instance, for the current bundle
    private transient List<Row> bundleBatch = new ArrayList<>();;
    private transient @Nullable String currentBundleKey = null;

    public StaticVarBatchingDoFnSingleKey(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
    }

    @StartBundle
    public void startBundle() {
      bundleBatch.clear();
      currentBundleKey = null;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (currentBundleKey == null) {
        currentBundleKey = c.element().getKey();
      }
      bundleBatch.add(c.element().getValue());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      if (bundleBatch.isEmpty()) {
        return;
      }

      List<Row> batchToEmit = null;
      String keyForEmit = null;

      synchronized (staticBatchLock) {
        if (globalBatchKey == null) {
          globalBatchKey = currentBundleKey;
        }

        staticBatch.addAll(bundleBatch);

        if (staticBatch.size() >= maxBatchSize) {
          batchToEmit = new ArrayList<>(staticBatch);
          staticBatch.clear();
          ((ArrayList<Row>) staticBatch).ensureCapacity(maxBatchSize);

          keyForEmit = globalBatchKey;
        }
      }
      bundleBatch.clear();

      if (batchToEmit != null && keyForEmit != null) {
        c.output(
            KV.of(ShardedKey.of(keyForEmit, EMPTY_SHARD_VALUE), batchToEmit),
            Instant.now(),
            GlobalWindow.INSTANCE);
      }
    }

    @Teardown
    public void tearDown() {
      synchronized (staticBatchLock) {
        int remaining = staticBatch.size();
        if (remaining > 0) {
          LOG.warn(
              "StaticVarBatchingDoFnSingleKey @TearDown: {} stranded records for key '{}'.",
              remaining,
              globalBatchKey);
        } else {
          LOG.info("StaticVarBatchingDoFnSingleKey @TearDown: No stranded records.");
        }
      }
    }
  }
}
