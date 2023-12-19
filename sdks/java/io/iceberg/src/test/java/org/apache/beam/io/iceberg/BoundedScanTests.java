package org.apache.beam.io.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.io.iceberg.util.SchemaHelper;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BoundedScanTests {

  private static Logger LOG = LoggerFactory.getLogger(BoundedScanTests.class);
  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestDataWarehouse warehouse = new TestDataWarehouse(temporaryFolder,"default");

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  static class PrintRow extends DoFn<Row,Row> {

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> output) throws Exception {
      LOG.info("Got row {}",row);
      output.output(row);
    }
  }

  @Test
  public void testSimpleScan() throws Exception {
    Table simpleTable = warehouse.createTable(TestFixtures.SCHEMA);
    simpleTable.newFastAppend()
        .appendFile(warehouse.writeRecords("file1s1.parquet",simpleTable.schema(),TestFixtures.FILE1SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file2s1.parquet",simpleTable.schema(),TestFixtures.FILE2SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file3s1.parquet",simpleTable.schema(),TestFixtures.FILE3SNAPSHOT1))
        .commit();

    PCollection<Row> output = testPipeline
        .apply(Create.of(IcebergScan.builder()
                .catalogName("hadoop")
                .catalogConfiguration(ImmutableMap.of(
                        CatalogUtil.ICEBERG_CATALOG_TYPE,CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, // Filesystem
                        CatalogProperties.WAREHOUSE_LOCATION, warehouse.location // Directory where our temp warehouse lives
                ))
                .table(simpleTable.name().replace("hadoop.","")) // Catalog name shouldn't be included
                .scanType(IcebergScan.ScanType.TABLE) // Do a normal scan.
                .build()).withCoder(SerializableCoder.of(IcebergScan.class)))
        .apply(ParDo.of(new IcebergScanGeneratorFn()))
        .apply(ParDo.of(new IcebergFileScanFn(TestFixtures.SCHEMA)))
        .setCoder(RowCoder.of(SchemaHelper.convert(TestFixtures.SCHEMA)))
        .apply(ParDo.of(new PrintRow()))
        .setCoder(RowCoder.of(SchemaHelper.convert(TestFixtures.SCHEMA)));
    PAssert.that(output);
    testPipeline.run();


  }

}
