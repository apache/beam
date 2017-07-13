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

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.annotations.VisibleForTesting;

/** A simplest read function implementation. Parallelism support is coming. */
@VisibleForTesting
class NaiveSpannerReadFn extends AbstractSpannerFn<Object, Struct> {
  private final SpannerIO.Read config;

  NaiveSpannerReadFn(SpannerIO.Read config) {
    this.config = config;
  }

  SpannerConfig getSpannerConfig() {
    return config.getSpannerConfig();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    TimestampBound timestampBound = TimestampBound.strong();
    if (config.getTransaction() != null) {
      Transaction transaction = c.sideInput(config.getTransaction());
      timestampBound = TimestampBound.ofReadTimestamp(transaction.timestamp());
    }
    try (ReadOnlyTransaction readOnlyTransaction =
        databaseClient().readOnlyTransaction(timestampBound)) {
      ResultSet resultSet = execute(readOnlyTransaction);
      while (resultSet.next()) {
        c.output(resultSet.getCurrentRowAsStruct());
      }
    }
  }

  private ResultSet execute(ReadOnlyTransaction readOnlyTransaction) {
    if (config.getQuery() != null) {
      return readOnlyTransaction.executeQuery(config.getQuery());
    }
    if (config.getIndex() != null) {
      return readOnlyTransaction.readUsingIndex(
          config.getTable(), config.getIndex(), config.getKeySet(), config.getColumns());
    }
    return readOnlyTransaction.read(config.getTable(), config.getKeySet(), config.getColumns());
  }
}
