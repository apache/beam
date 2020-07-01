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

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import org.apache.beam.sdk.transforms.DoFn;

/** Creates a batch transaction. */
class CreateTransactionFn extends DoFn<Object, Transaction> {

  private final SpannerIO.CreateTransaction config;

  CreateTransactionFn(SpannerIO.CreateTransaction config) {
    this.config = config;
  }

  private transient SpannerAccessor spannerAccessor;

  @DoFn.Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(config.getSpannerConfig());
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    BatchReadOnlyTransaction tx =
        spannerAccessor.getBatchClient().batchReadOnlyTransaction(config.getTimestampBound());
    c.output(Transaction.create(tx.getBatchTransactionId()));
  }
}
