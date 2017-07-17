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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * This transform takes in key-value pairs of {@link TableRow} entries and the
 * {@link TableDestination} it should be written to. The BigQuery streaming-write service is used
 * to stream these writes to the appropriate table.
 *
 * <p>This transform assumes that all destination tables already exist by the time it sees a write
 * for that table.
 */
public class StreamingWriteTables extends PTransform<
    PCollection<KV<TableDestination, TableRow>>, WriteResult> {
  private BigQueryServices bigQueryServices;
  private InsertRetryPolicy retryPolicy;

  public StreamingWriteTables() {
    this(new BigQueryServicesImpl(), InsertRetryPolicy.alwaysRetry());
  }

  private StreamingWriteTables(BigQueryServices bigQueryServices, InsertRetryPolicy retryPolicy) {
    this.bigQueryServices = bigQueryServices;
    this.retryPolicy = retryPolicy;
  }

  StreamingWriteTables withTestServices(BigQueryServices bigQueryServices) {
    return new StreamingWriteTables(bigQueryServices, retryPolicy);
  }

  StreamingWriteTables withInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
    return new StreamingWriteTables(bigQueryServices, retryPolicy);
  }

  @Override
  public WriteResult expand(PCollection<KV<TableDestination, TableRow>> input) {
    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.

    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates
    // We create 50 keys per BigQuery table to generate output on. This is few enough that we
    // get good batching into BigQuery's insert calls, and enough that we can max out the
    // streaming insert quota.
    PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged =
        input.apply("ShardTableWrites", ParDo.of
        (new GenerateShardedTable(50)))
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowJsonCoder.of()))
        .apply("TagWithUniqueIds", ParDo.of(new TagWithUniqueIds()));

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
    // performed by Reshuffle.
    TupleTag<Void> mainOutputTag = new TupleTag<>("mainOutput");
    TupleTag<TableRow> failedInsertsTag = new TupleTag<>("failedInserts");
    PCollectionTuple tuple = tagged
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
        .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
        // Put in the global window to ensure that DynamicDestinations side inputs are accessed
        // correctly.
        .apply("GlobalWindow",
            Window.<KV<ShardedKey<String>, TableRowInfo>>into(new GlobalWindows())
            .triggering(DefaultTrigger.of()).discardingFiredPanes())
        .apply("StreamingWrite",
            ParDo.of(
                new StreamingWriteFn(bigQueryServices, retryPolicy, failedInsertsTag))
            .withOutputTags(mainOutputTag, TupleTagList.of(failedInsertsTag)));
    PCollection<TableRow> failedInserts = tuple.get(failedInsertsTag);
    failedInserts.setCoder(TableRowJsonCoder.of());
    return WriteResult.in(input.getPipeline(), failedInsertsTag, failedInserts);
  }
}
