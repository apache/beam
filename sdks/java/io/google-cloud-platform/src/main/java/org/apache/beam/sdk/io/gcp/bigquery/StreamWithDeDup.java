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

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonSchemaToTableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
* PTransform that performs streaming BigQuery write. To increase consistency,
* it leverages BigQuery best effort de-dup mechanism.
 */
class StreamWithDeDup<T> extends PTransform<PCollection<T>, WriteResult> {
  private final Write<T> write;

  /** Constructor. */
  StreamWithDeDup(Write<T> write) {
    this.write = write;
  }

  @Override
  protected Coder<Void> getDefaultOutputCoder() {
    return VoidCoder.of();
  }

  @Override
  public WriteResult expand(PCollection<T> input) {
    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.

    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates.

    PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged =
        input.apply(ParDo.of(new TagWithUniqueIdsAndTable<T>(
            input.getPipeline().getOptions().as(BigQueryOptions.class), write.getTable(),
            write.getTableRefFunction(), write.getFormatFunction())));

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
    // performed by Reshuffle.
    NestedValueProvider<TableSchema, String> schema =
        write.getJsonSchema() == null
            ? null
            : NestedValueProvider.of(write.getJsonSchema(), new JsonSchemaToTableSchema());
    tagged
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
        .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
        .apply(
            ParDo.of(
                new StreamingWriteFn(
                    schema,
                    write.getCreateDisposition(),
                    write.getTableDescription(),
                    write.getBigQueryServices())));
    return WriteResult.in(input.getPipeline());
  }
}
