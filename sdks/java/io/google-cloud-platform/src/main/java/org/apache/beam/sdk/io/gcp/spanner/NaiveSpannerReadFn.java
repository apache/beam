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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/** A simplest read function implementation. Parallelism support is coming. */
@VisibleForTesting
class NaiveSpannerReadFn extends DoFn<ReadOperation, Struct> {
  private final SpannerConfig config;
  @Nullable private final PCollectionView<Transaction> transaction;
  private transient SpannerAccessor spannerAccessor;

  NaiveSpannerReadFn(SpannerConfig config, @Nullable PCollectionView<Transaction> transaction) {
    this.config = config;
    this.transaction = transaction;
  }

  NaiveSpannerReadFn(SpannerConfig config) {
    this(config, null);
  }


  @Setup
  public void setup() throws Exception {
    spannerAccessor = config.connectToSpanner();
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    TimestampBound timestampBound = TimestampBound.strong();
    if (transaction != null) {
      Transaction transaction = c.sideInput(this.transaction);
      timestampBound = TimestampBound.ofReadTimestamp(transaction.timestamp());
    }
    ReadOperation op = c.element();
    DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
    try (ReadOnlyTransaction readOnlyTransaction =
        databaseClient.readOnlyTransaction(timestampBound)) {
      ResultSet resultSet = execute(op, readOnlyTransaction);
      while (resultSet.next()) {
        c.output(resultSet.getCurrentRowAsStruct());
      }
    }
  }

  private ResultSet execute(ReadOperation op, ReadOnlyTransaction readOnlyTransaction) {
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
