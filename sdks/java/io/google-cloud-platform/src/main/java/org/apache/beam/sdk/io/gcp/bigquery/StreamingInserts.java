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
import com.google.api.services.bigquery.model.TableSchema;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
* PTransform that performs streaming BigQuery write. To increase consistency,
* it leverages BigQuery best effort de-dup mechanism.
 */

class StreamingInserts
    extends PTransform<PCollection<KV<TableDestination, TableRow>>, WriteResult> {
  private final Write<?> write;

  private static class ConstantSchemaFunction implements
      SerializableFunction<TableDestination, TableSchema> {
    private final @Nullable String jsonSchema;

    ConstantSchemaFunction(TableSchema schema) {
      this.jsonSchema = BigQueryHelpers.toJsonString(schema);
    }

    @Override
    @Nullable
    public TableSchema apply(TableDestination table) {
      return BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    }
  }

  /** Constructor. */
  StreamingInserts(Write<?> write) {
    this.write = write;
  }

  @Override
  protected Coder<Void> getDefaultOutputCoder() {
    return VoidCoder.of();
  }

  @Override
  public WriteResult expand(PCollection<KV<TableDestination, TableRow>> input) {
    // Since BigQueryIO.java does not yet have support for per-table schemas, inject a constant
    // schema function here. If no schema is specified, this function will return null.
    SerializableFunction<TableDestination, TableSchema> schemaFunction =
        new ConstantSchemaFunction(write.getSchema());

    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.

    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates.
    PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged = input
        .apply("CreateTables", ParDo.of(new CreateTables(write.getCreateDisposition(),
            write.getBigQueryServices(), schemaFunction)))
        // We create 50 keys per BigQuery table to generate output on. This is few enough that we
        // get good batching into BigQuery's insert calls, and enough that we can max out the
        // streaming insert quota.
        .apply("ShardTableWrites", ParDo.of(new GenerateShardedTable(50)))
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowJsonCoder.of()))
        .apply("TagWithUniqueIds", ParDo.of(new TagWithUniqueIds()));

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
    // performed by Reshuffle.
    tagged
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
        .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
        .apply("StreamingWrite",
            ParDo.of(
                new StreamingWriteFn(write.getBigQueryServices())));

    return WriteResult.in(input.getPipeline());
  }
}
