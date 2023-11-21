package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
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
        .apply(Create.of())
        .apply(ParDo.of(new IcebergScanGeneratorFn()));
    PAssert.that(output);
    testPipeline.run();


  }

}
