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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

class WriteToDestinations extends PTransform<PCollection<Row>, IcebergWriteResult> {

  static final long DEFAULT_MAX_BYTES_PER_FILE = (1L << 40); // 1TB
  static final int DEFAULT_NUM_FILE_SHARDS = 0;
  static final int FILE_TRIGGERING_RECORD_COUNT = 50_000;

  private final IcebergCatalogConfig catalogConfig;
  private final DynamicDestinations dynamicDestinations;

  WriteToDestinations(IcebergCatalogConfig catalogConfig, DynamicDestinations dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
  }

  @Override
  public IcebergWriteResult expand(PCollection<Row> input) {

    // First, attempt to write directly to files without shuffling. If there are
    // too many distinct destinations in a single bundle, the remaining
    // elements will be emitted to take the "slow path" that involves a shuffle
    WriteUngroupedRowsToFiles.Result writeUngroupedResult =
        input.apply(
            "Fast-path write rows",
            new WriteUngroupedRowsToFiles(catalogConfig, dynamicDestinations));

    // Then write the rest by shuffling on the destination metadata
    Preconditions.checkState(
        writeUngroupedResult.getSpilledRows().getSchema().hasField("dest"),
        "Input schema missing `dest` field.");
    Schema dataSchema =
        checkArgumentNotNull(
            writeUngroupedResult
                .getSpilledRows()
                .getSchema()
                .getField("data")
                .getType()
                .getRowSchema(),
            "Input schema missing `data` field");

    PCollection<FileWriteResult> writeGroupedResult =
        writeUngroupedResult
            .getSpilledRows()
            .apply(
                "Key by destination and shard",
                MapElements.via(
                    new SimpleFunction<Row, KV<ShardedKey<String>, Row>>() {
                      private static final int SPILLED_ROWS_SHARDING_FACTOR = 10;
                      private int shardNumber = 0;

                      @Override
                      public KV<ShardedKey<String>, Row> apply(Row elem) {
                        Row data =
                            checkArgumentNotNull(
                                elem.getRow("data"), "Element missing `data` field");
                        String dest =
                            checkArgumentNotNull(
                                elem.getString("dest"), "Element missing `dest` field");
                        return KV.of(
                            ShardedKey.of(dest, shardNumber % SPILLED_ROWS_SHARDING_FACTOR), data);
                      }
                    }))
            .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), RowCoder.of(dataSchema)))
            .apply("Group spilled rows by destination shard", GroupByKey.create())
            .apply(
                "Write remaining rows to files",
                new WriteGroupedRowsToFiles(catalogConfig, dynamicDestinations));

    PCollection<FileWriteResult> allWrittenFiles =
        PCollectionList.of(writeUngroupedResult.getWrittenFiles())
            .and(writeGroupedResult)
            .apply("Flatten Written Files", Flatten.pCollections());

    // Apply any sharded writes and flatten everything for catalog updates
    PCollection<KV<String, SnapshotInfo>> snapshots =
        allWrittenFiles.apply(new AppendFilesToTables(catalogConfig));

    return new IcebergWriteResult(input.getPipeline(), snapshots);
  }
}
