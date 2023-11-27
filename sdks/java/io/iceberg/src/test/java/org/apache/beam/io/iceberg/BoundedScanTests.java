package org.apache.beam.io.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BoundedScanTests {
  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestDataWarehouse warehouse = new TestDataWarehouse(temporaryFolder,"default");

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testSimpleScan() throws Exception {
    Table simpleTable = warehouse.createTable(TestFixtures.SCHEMA);
    simpleTable.newFastAppend()
        .appendFile(warehouse.writeRecords("file1s1.parquet",simpleTable.schema(),TestFixtures.FILE1SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file2s1.parquet",simpleTable.schema(),TestFixtures.FILE2SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file3s1.parquet",simpleTable.schema(),TestFixtures.FILE3SNAPSHOT1))
        .commit();

    PCollection<IcebergScanTask> output = testPipeline
        .apply(Create.of(IcebergScan.builder()
                .catalogName("hadoop")
                .catalogConfiguration(ImmutableMap.of(
                        CatalogUtil.ICEBERG_CATALOG_TYPE,CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, // Filesystem
                        CatalogProperties.WAREHOUSE_LOCATION, warehouse.location // Directory where our temp warehouse lives
                ))
                .table(simpleTable.name().replace("hadoop.","")) // Catalog name shouldn't be included
                .scanType(IcebergScan.ScanType.TABLE) // Do a normal scan.
                .build()))
        .apply(ParDo.of(new IcebergScanGeneratorFn()));
    PAssert.that(output);
    testPipeline.run();


  }

}
