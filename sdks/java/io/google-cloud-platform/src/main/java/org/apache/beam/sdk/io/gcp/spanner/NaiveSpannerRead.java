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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A naive version of Spanner read that doesn't use the Batch API. */
@VisibleForTesting
@AutoValue
abstract class NaiveSpannerRead
    extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

  public static NaiveSpannerRead create(
      SpannerConfig spannerConfig,
      PCollectionView<Transaction> txView,
      TimestampBound timestampBound) {
    return new AutoValue_NaiveSpannerRead(spannerConfig, txView, timestampBound);
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

    return input.apply(
        "Naive read from Cloud Spanner",
        ParDo.of(new NaiveSpannerReadFn(getSpannerConfig(), txView)).withSideInputs(txView));
  }

  private static class NaiveSpannerReadFn extends DoFn<ReadOperation, Struct> {

    private final SpannerConfig config;
    private final @Nullable PCollectionView<Transaction> txView;
    private transient SpannerAccessor spannerAccessor;

    NaiveSpannerReadFn(SpannerConfig config, @Nullable PCollectionView<Transaction> transaction) {
      this.config = config;
      this.txView = transaction;
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
      ReadOperation op = c.element();
      BatchReadOnlyTransaction context =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());
      try (ResultSet resultSet = execute(op, context)) {
        while (resultSet.next()) {
          c.output(resultSet.getCurrentRowAsStruct());
        }
      }
    }

    private ResultSet execute(ReadOperation op, BatchReadOnlyTransaction readOnlyTransaction) {
      if (op.getQuery() != null) {
        return readOnlyTransaction.executeQuery(op.getQuery());
      }
      if (op.getIndex() != null) {
        return readOnlyTransaction.readUsingIndex(
            op.getTable(), op.getIndex(), op.getKeySet(), op.getColumns());
      }
      return readOnlyTransaction.read(op.getTable(), op.getKeySet(), op.getColumns());
    }
  }
}
