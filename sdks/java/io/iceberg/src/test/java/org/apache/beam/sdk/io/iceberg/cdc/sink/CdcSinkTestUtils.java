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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;

/**
 * Shared test helpers for the {@code cdc/sink} suites. The TableCache and catalog caches are
 * process-wide statics, so tests must use unique table names per test method.
 */
final class CdcSinkTestUtils {

  private CdcSinkTestUtils() {}

  /** An in-process {@link HadoopCatalog} rooted at {@code warehouseDir}. */
  static Catalog hadoopCatalog(File warehouseDir) {
    Configuration hadoopConf = new Configuration();
    return new HadoopCatalog(hadoopConf, warehouseDir.getAbsolutePath());
  }

  /** An {@link IcebergCatalogConfig} resolving to the same warehouse as {@link #hadoopCatalog}. */
  static IcebergCatalogConfig catalogConfig(File warehouseDir) {
    return IcebergCatalogConfig.builder()
      .setCatalogProperties(
        ImmutableMap.of(
          "type",
          CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
          "warehouse",
          "file:" + warehouseDir.getAbsolutePath()))
      .build();
  }

  /** Creates a table with the given identifier field ids format version. */
  static Table createTable(
    Catalog catalog,
    TableIdentifier id,
    Schema schema,
    Set<Integer> identifierFieldIds,
    int formatVersion,
    PartitionSpec spec) {
    Schema schemaWithIds = new Schema(schema.columns(), identifierFieldIds);
    Map<String, String> props = ImmutableMap.of("format-version", String.valueOf(formatVersion));
    return catalog.createTable(id, schemaWithIds, spec, props);
  }

  /**
   * Creates the two fresh unpartitioned V2 routing targets {@code db.<tableA>} and {@code
   * db.<tableB>}, both with columns {@code (id INT pk, dest STRING)}: the dynamic-destination
   * fixture where the routing column is also a data column.
   */
  static void createDestTables(Catalog catalog, String tableA, String tableB) {
    Schema destTableSchema =
      new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "dest", Types.StringType.get()));
    for (String name : ImmutableList.of(tableA, tableB)) {
      createTable(
        catalog,
        TableIdentifier.of("db", name),
        destTableSchema,
        ImmutableSet.of(1),
        2,
        PartitionSpec.unpartitioned());
    }
  }

  /**
   * Creates a table born WITH {@code sortOrder}, distinct from altering afterwards: such a table
   * stores only sort order id 1, no id 0 (the id every sink equality delete carries).
   */
  static Table createSortedTable(
    Catalog catalog,
    TableIdentifier id,
    Schema schema,
    Set<Integer> identifierFieldIds,
    int formatVersion,
    PartitionSpec spec,
    SortOrder sortOrder) {
    Schema schemaWithIds = new Schema(schema.columns(), identifierFieldIds);
    return catalog
      .buildTable(id, schemaWithIds)
      .withPartitionSpec(spec)
      .withSortOrder(sortOrder)
      .withProperties(ImmutableMap.of("format-version", String.valueOf(formatVersion)))
      .create();
  }

  /**
   * A {@link RecordDeltaTaskWriter} through the production factory path: table-resolved formats,
   * the current spec as the pinned spec.
   */
  static RecordDeltaTaskWriter deltaWriter(
    Table table, Set<Integer> equalityFieldIds, boolean upsert, long targetFileSizeBytes) {
    FileFormat dataFormat = RecordDeltaTaskWriter.dataFileFormat(table);
    FileFormat deleteFormat = RecordDeltaTaskWriter.deleteFileFormat(table, dataFormat);
    return RecordDeltaTaskWriter.create(
      table,
      table.spec(),
      equalityFieldIds,
      upsert,
      targetFileSizeBytes,
      OutputFileFactory.builderFor(table, 1, 1).build(),
      dataFormat,
      deleteFormat);
  }

  /** An {@link DoFn.OutputReceiver} appending to {@code out}, for driving a DoFn directly. */
  static <T> DoFn.OutputReceiver<T> collectInto(List<T> out) {
    return new DoFn.OutputReceiver<T>() {
      @Override
      public OutputBuilder<T> builder(T value) {
        throw new UnsupportedOperationException("test receiver: use output(value)");
      }

      @Override
      public void output(T value) {
        out.add(value);
      }
    };
  }

  /** Commits a {@link WriteResult}'s data and delete files to the table as one row delta. */
  static void commitRowDelta(Table table, WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }

  /** Attaches each element's {@link ValueKind} to its {@link Row}: the sink's input contract. */
  static PCollection<Row> withKinds(PCollection<KV<ValueKind, Row>> tagged) {
    return tagged.apply(kindsFn());
  }

  /** {@link #withKinds(PCollection)} with an explicit step name, for multi-application tests. */
  static PCollection<Row> withKinds(String name, PCollection<KV<ValueKind, Row>> tagged) {
    return tagged.apply(name, kindsFn());
  }

  private static ParDo.SingleOutput<KV<ValueKind, Row>, Row> kindsFn() {
    return ParDo.of(
      new DoFn<KV<ValueKind, Row>, Row>() {
        @ProcessElement
        public void process(@Element KV<ValueKind, Row> e, OutputReceiver<Row> out) {
          out.builder(e.getValue()).setValueKind(e.getKey()).output();
        }
      });
  }
}
