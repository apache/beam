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

import static org.apache.beam.sdk.io.iceberg.TestFixtures.createRecord;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class IcebergIOReadTest {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOReadTest.class);

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {String.format("{\"namespace\": [\"default\"], \"name\": \"%s\"}", tableId())},
          {String.format("default.%s", tableId())},
        });
  }

  public static String tableId() {
    return "table" + Long.toString(UUID.randomUUID().hashCode(), 16);
  }

  @Parameterized.Parameter public String tableStringIdentifier;

  static class PrintRow extends DoFn<Row, Row> {

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> output) throws Exception {
      LOG.info("Got row {}", row);
      output.output(row);
    }
  }

  @Test
  public void testSimpleScan() throws Exception {
    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableStringIdentifier);
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file2s1.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file3s1.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT1))
        .commit();

    final List<Row> expectedRows =
        Stream.of(
                TestFixtures.FILE1SNAPSHOT1,
                TestFixtures.FILE2SNAPSHOT1,
                TestFixtures.FILE3SNAPSHOT1)
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    PCollection<Row> output =
        testPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA)));

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testIdentityColumnScan() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    String identityColumnName = "identity";
    String identityColumnValue = "some-value";
    simpleTable.updateSchema().addColumn(identityColumnName, Types.StringType.get()).commit();
    simpleTable.updateSpec().addField(identityColumnName).commit();

    PartitionSpec spec = simpleTable.spec();
    PartitionKey partitionKey = new PartitionKey(simpleTable.spec(), simpleTable.schema());
    partitionKey.set(0, identityColumnValue);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet",
                TestFixtures.SCHEMA,
                spec,
                partitionKey,
                TestFixtures.FILE1SNAPSHOT1))
        .commit();

    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema());
    final List<Row> expectedRows =
        Stream.of(TestFixtures.FILE1SNAPSHOT1_DATA)
            .flatMap(List::stream)
            .map(
                d ->
                    ImmutableMap.<String, Object>builder()
                        .putAll(d)
                        .put(identityColumnName, identityColumnValue)
                        .build())
            .map(r -> createRecord(simpleTable.schema(), r))
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    PCollection<Row> output =
        testPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema())));

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testNameMappingScan() throws Exception {
    org.apache.avro.Schema metadataSchema =
        org.apache.avro.Schema.createRecord(
            "metadata",
            null,
            null,
            false,
            ImmutableList.of(
                new org.apache.avro.Schema.Field(
                    "source",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                    null,
                    null)));

    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord(
            "test",
            null,
            null,
            false,
            ImmutableList.of(
                new org.apache.avro.Schema.Field(
                    "data",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                    null,
                    null),
                new org.apache.avro.Schema.Field(
                    "id",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                    null,
                    null),
                new org.apache.avro.Schema.Field("metadata", metadataSchema, null, null)));

    List<Map<String, Object>> recordData =
        ImmutableList.<Map<String, Object>>builder()
            .add(
                ImmutableMap.of(
                    "id",
                    0L,
                    "data",
                    "clarification",
                    "metadata",
                    ImmutableMap.of("source", "systemA")))
            .add(
                ImmutableMap.of(
                    "id", 1L, "data", "risky", "metadata", ImmutableMap.of("source", "systemB")))
            .add(
                ImmutableMap.of(
                    "id", 2L, "data", "falafel", "metadata", ImmutableMap.of("source", "systemC")))
            .build();

    List<GenericRecord> avroRecords =
        recordData.stream()
            .map(data -> avroGenericRecord(avroSchema, data))
            .collect(Collectors.toList());

    Configuration hadoopConf = new Configuration();
    String path = createParquetFile(avroSchema, avroRecords);
    HadoopInputFile inputFile = HadoopInputFile.fromLocation(path, hadoopConf);

    NameMapping defaultMapping =
        NameMapping.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "metadata", MappedFields.of(MappedField.of(4, "source"))));
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.<String, String>builder()
            .put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(defaultMapping))
            .build();

    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable =
        warehouse
            .buildTable(tableId, TestFixtures.NESTED_SCHEMA)
            .withProperties(tableProperties)
            .withPartitionSpec(PartitionSpec.unpartitioned())
            .create();

    MetricsConfig metricsConfig = MetricsConfig.forTable(simpleTable);
    Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat(FileFormat.PARQUET)
            .withInputFile(inputFile)
            .withMetrics(metrics)
            .build();

    final Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.NESTED_SCHEMA);

    simpleTable.newFastAppend().appendFile(dataFile).commit();

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    PCollection<Row> output =
        testPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(beamSchema));

    final Row[] expectedRows =
        recordData.stream()
            .map(data -> icebergGenericRecord(TestFixtures.NESTED_SCHEMA.asStruct(), data))
            .map(record -> IcebergUtils.icebergRecordToBeamRow(beamSchema, record))
            .toArray(Row[]::new);

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows));
              return null;
            });

    testPipeline.run();
  }

  @SuppressWarnings("unchecked")
  public static GenericRecord avroGenericRecord(
      org.apache.avro.Schema schema, Map<String, Object> values) {
    GenericRecord record = new GenericData.Record(schema);
    for (org.apache.avro.Schema.Field field : schema.getFields()) {
      Object rawValue = values.get(field.name());
      Object avroValue =
          rawValue instanceof Map
              ? avroGenericRecord(field.schema(), (Map<String, Object>) rawValue)
              : rawValue;
      record.put(field.name(), avroValue);
    }
    return record;
  }

  @SuppressWarnings("unchecked")
  public static Record icebergGenericRecord(Types.StructType type, Map<String, Object> values) {
    org.apache.iceberg.data.GenericRecord record =
        org.apache.iceberg.data.GenericRecord.create(type);
    for (Types.NestedField field : type.fields()) {
      Object rawValue = values.get(field.name());
      Object value =
          rawValue instanceof Map
              ? icebergGenericRecord(field.type().asStructType(), (Map<String, Object>) rawValue)
              : rawValue;
      record.setField(field.name(), value);
    }
    return record;
  }

  public static String createParquetFile(org.apache.avro.Schema schema, List<GenericRecord> records)
      throws IOException {

    File tempFile = createTempFile();
    Path file = new Path(tempFile.getPath());

    AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(file);
    ParquetWriter<GenericRecord> parquetWriter = builder.withSchema(schema).build();
    for (GenericRecord record : records) {
      parquetWriter.write(record);
    }
    parquetWriter.close();

    return tempFile.getPath();
  }

  private static File createTempFile() throws IOException {
    File tempFile = File.createTempFile(ScanSourceTest.class.getSimpleName(), ".tmp");
    tempFile.deleteOnExit();
    boolean unused = tempFile.delete();
    return tempFile;
  }
}
