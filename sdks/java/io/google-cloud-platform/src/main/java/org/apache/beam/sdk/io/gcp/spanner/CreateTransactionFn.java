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
import com.google.cloud.spanner.Statement;

/** Creates a batch transaction. */
class CreateTransactionFn extends AbstractSpannerFn<Object, Transaction> {

  private static final long serialVersionUID = -4174426331092286581L;

  private final SpannerIO.CreateTransaction config;

  CreateTransactionFn(SpannerIO.CreateTransaction config) {
    this.config = config;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    try (ReadOnlyTransaction readOnlyTransaction =
        databaseClient().readOnlyTransaction(config.getTimestampBound())) {
      // Run a dummy sql statement to force the RPC and obtain the timestamp from the server.
      ResultSet resultSet = readOnlyTransaction.executeQuery(Statement.of("SELECT 1"));
      while (resultSet.next()) {
        // do nothing
      }
      Transaction tx = Transaction.create(readOnlyTransaction.getReadTimestamp());
      c.output(tx);
    }
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return config.getSpannerConfig();
  }
}
