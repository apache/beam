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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end dynamic destinations against a local Hadoop catalog. */
@RunWith(JUnit4.class)
public class AddFilesDynamicDestinationsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private String root;
  private HadoopCatalog catalog;
  private IcebergCatalogConfig catalogConfig;

  private static final Schema ORDERS =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "item", Types.StringType.get()));

  @Before
  public void setup() {
    root = temp.getRoot().getAbsolutePath() + "/";
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
  }

  private SchemaEvolutionConfig evolution() {
    return SchemaEvolutionConfig.builder()
        .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
        .build();
  }

  private String write(String relative, Schema schema, Record... records) throws IOException {
    String file = root + relative;
    new File(file).getParentFile().mkdirs();
    DataWriter<Record> writer =
        Parquet.writeData(org.apache.iceberg.Files.localOutput(file))
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private static Record order(long id, String item) {
    Record record = GenericRecord.create(ORDERS);
    record.setField("id", id);
    record.setField("item", item);
    return record;
  }

  private PCollectionRowTuple run(String pattern, String template, List<String> files) {
    AddFiles addFiles =
        new AddFiles(catalogConfig, template, null, null, null, null, null, null, evolution())
            .withPathPattern(pattern);
    return pipeline.apply("Files", Create.of(files)).apply(addFiles);
  }

  private long rowCount(TableIdentifier id) {
    if (!catalog.tableExists(id)) {
      return -1;
    }
    Table table = catalog.loadTable(id);
    if (table.currentSnapshot() == null) {
      return 0;
    }
    long total = 0;
    for (org.apache.iceberg.DataFile file : table.currentSnapshot().addedDataFiles(table.io())) {
      total += file.recordCount();
    }
    return total;
  }

  /** Three tables under one pattern: all three load, each only its own files. */
  @Test
  public void threeTablesEachGetOnlyItsOwnFiles() throws Exception {
    catalog.createNamespace(Namespace.of("lake"));
    List<String> files =
        Arrays.asList(
            write("export/orders/a.parquet", ORDERS, order(1, "x"), order(2, "y")),
            write("export/orders/b.parquet", ORDERS, order(3, "z")),
            write("export/customers/a.parquet", ORDERS, order(4, "c")),
            write("export/events/a.parquet", ORDERS, order(5, "e")));

    PCollectionRowTuple out = run(root + "export/{tbl}/**", "lake.{tbl}", files);
    PAssert.that(out.get(AddFiles.ERROR_TAG)).empty();
    pipeline.run().waitUntilFinish();

    assertEquals(3, rowCount(TableIdentifier.of("lake", "orders")));
    assertEquals(1, rowCount(TableIdentifier.of("lake", "customers")));
    assertEquals(1, rowCount(TableIdentifier.of("lake", "events")));
  }

  /** A path that matches nothing is an error row, and does not stop the others. */
  @Test
  public void anUnmatchedPathDoesNotStopTheRest() throws Exception {
    catalog.createNamespace(Namespace.of("lake"));
    String stray = write("stray.parquet", ORDERS, order(9, "s"));
    List<String> files =
        Arrays.asList(write("export/orders/a.parquet", ORDERS, order(1, "x")), stray);

    PCollectionRowTuple out = run(root + "export/{tbl}/**", "lake.{tbl}", files);
    PAssert.that(out.get(AddFiles.ERROR_TAG))
        .satisfies(
            rows -> {
              List<Row> all = new ArrayList<>();
              rows.forEach(all::add);
              assertEquals("expected exactly the stray: " + all, 1, all.size());
              assertEquals(stray, all.get(0).getString("file"));
              assertEquals("", all.get(0).getString("table"));
              return null;
            });
    pipeline.run().waitUntilFinish();

    assertEquals(1, rowCount(TableIdentifier.of("lake", "orders")));
  }

  /** The invariant that matters: every input file either registers or is reported. */
  @Test
  public void everyFileEitherRegistersOrIsReported() throws Exception {
    catalog.createNamespace(Namespace.of("lake"));
    Set<String> inputs = new HashSet<>();
    inputs.add(write("export/orders/a.parquet", ORDERS, order(1, "x")));
    inputs.add(write("export/orders/b.parquet", ORDERS, order(2, "y")));
    inputs.add(write("nope/c.parquet", ORDERS, order(3, "z")));

    run(root + "export/{tbl}/**", "lake.{tbl}", new ArrayList<>(inputs));
    pipeline.run().waitUntilFinish();

    Table orders = catalog.loadTable(TableIdentifier.of("lake", "orders"));
    Set<String> registered = new HashSet<>();
    for (org.apache.iceberg.DataFile file : orders.currentSnapshot().addedDataFiles(orders.io())) {
      registered.add(file.path().toString());
    }
    assertEquals(2, registered.size());
    assertTrue(registered.contains(root + "export/orders/a.parquet"));
    assertFalse(registered.contains(root + "nope/c.parquet"));
  }

  /** The central question: one table refused, the other two load completely. */
  @Test
  public void aRefusedTableDoesNotStopTheOthers() throws Exception {
    catalog.createNamespace(Namespace.of("lake"));
    // customers exists with id as a long; its file declares id as a string, which cannot merge
    Schema conflicting =
        new Schema(
            required(1, "id", Types.StringType.get()), optional(2, "item", Types.StringType.get()));
    catalog.createTable(TableIdentifier.of("lake", "customers"), ORDERS);

    Record bad = GenericRecord.create(conflicting);
    bad.setField("id", "not-a-long");
    bad.setField("item", "c");

    List<String> files =
        Arrays.asList(
            write("export/orders/a.parquet", ORDERS, order(1, "x")),
            write("export/customers/a.parquet", conflicting, bad),
            write("export/events/a.parquet", ORDERS, order(5, "e")));

    run(root + "export/{tbl}/**", "lake.{tbl}", files);
    pipeline.run().waitUntilFinish();

    assertEquals("orders loaded", 1, rowCount(TableIdentifier.of("lake", "orders")));
    assertEquals("events loaded", 1, rowCount(TableIdentifier.of("lake", "events")));
    Table customers = catalog.loadTable(TableIdentifier.of("lake", "customers"));
    assertEquals("customers has no snapshot", null, customers.currentSnapshot());
    assertEquals("customers schema untouched", ORDERS.asStruct(), customers.schema().asStruct());
  }

  /** A table whose files are all unreadable is refused rather than failing the run. */
  @Test
  public void aTableWithNothingReadableIsRefusedAlone() throws Exception {
    catalog.createNamespace(Namespace.of("lake"));
    File notParquet = new File(root + "export/junk/a.txt");
    notParquet.getParentFile().mkdirs();
    java.nio.file.Files.write(
        notParquet.toPath(), "not parquet".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    List<String> files =
        Arrays.asList(
            write("export/orders/a.parquet", ORDERS, order(1, "x")), notParquet.getAbsolutePath());

    run(root + "export/{tbl}/**", "lake.{tbl}", files);
    pipeline.run().waitUntilFinish();

    assertEquals("orders still loaded", 1, rowCount(TableIdentifier.of("lake", "orders")));
    assertFalse(
        "junk table was not created", catalog.tableExists(TableIdentifier.of("lake", "junk")));
  }
}
