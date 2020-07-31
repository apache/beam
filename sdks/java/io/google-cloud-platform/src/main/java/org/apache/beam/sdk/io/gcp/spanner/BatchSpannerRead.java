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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This transform reads from Cloud Spanner using the {@link com.google.cloud.spanner.BatchClient}.
 * Reads from multiple partitions are executed concurrently yet in the same read-only transaction.
 */
@AutoValue
abstract class BatchSpannerRead
    extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

  public static BatchSpannerRead create(
      SpannerConfig spannerConfig,
      PCollectionView<Transaction> txView,
      TimestampBound timestampBound) {
    return new AutoValue_BatchSpannerRead(spannerConfig, txView, timestampBound);
  }

  abstract SpannerConfig getSpannerConfig();

  abstract @Nullable PCollectionView<Transaction> getTxView();

  abstract TimestampBound getTimestampBound();

  @Override
  public PCollection<Struct> expand(PCollection<ReadOperation> input) {
    PCollectionView<Transaction> txView = getTxView();
    if (txView == null) {
      Pipeline begin = input.getPipeline();
      SpannerIO.CreateTransaction createTx =
          SpannerIO.createTransaction()
              .withSpannerConfig(getSpannerConfig())
              .withTimestampBound(getTimestampBound());
      txView = begin.apply(createTx);
    }
    return input
        .apply(
            "Generate Partitions",
            ParDo.of(new GeneratePartitionsFn(getSpannerConfig(), txView)).withSideInputs(txView))
        .apply("Shuffle partitions", Reshuffle.<Partition>viaRandomKey())
        .apply(
            "Read from Partitions",
            ParDo.of(new ReadFromPartitionFn(getSpannerConfig(), txView)).withSideInputs(txView));
  }

  @VisibleForTesting
  static class GeneratePartitionsFn extends DoFn<ReadOperation, Partition> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction> txView;

    private transient SpannerAccessor spannerAccessor;

    public GeneratePartitionsFn(
        SpannerConfig config, PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(config);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);
      BatchReadOnlyTransaction context =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());
      for (Partition p : execute(c.element(), context)) {
        c.output(p);
      }
    }

    private List<Partition> execute(ReadOperation op, BatchReadOnlyTransaction tx) {
      // Query was selected.
      if (op.getQuery() != null) {
        return tx.partitionQuery(op.getPartitionOptions(), op.getQuery());
      }
      // Read with index was selected.
      if (op.getIndex() != null) {
        return tx.partitionReadUsingIndex(
            op.getPartitionOptions(),
            op.getTable(),
            op.getIndex(),
            op.getKeySet(),
            op.getColumns());
      }
      // Read from table was selected.
      return tx.partitionRead(
          op.getPartitionOptions(), op.getTable(), op.getKeySet(), op.getColumns());
    }
  }

  private static class ReadFromPartitionFn extends DoFn<Partition, Struct> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction> txView;

    private transient SpannerAccessor spannerAccessor;

    public ReadFromPartitionFn(
        SpannerConfig config, PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(config);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);

      BatchReadOnlyTransaction batchTx =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());

      Partition p = c.element();
      try (ResultSet resultSet = batchTx.execute(p)) {
        while (resultSet.next()) {
          Struct s = resultSet.getCurrentRowAsStruct();
          c.output(s);
        }
      }
    }
  }
}
