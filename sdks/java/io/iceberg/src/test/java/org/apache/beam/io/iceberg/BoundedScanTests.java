package org.apache.beam.io.iceberg;

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

  @Test
  public void testSimpleScan() throws Exception {
    Table simpleTable = warehouse.createTable(TestFixtures.SCHEMA);
    simpleTable.newFastAppend()
        .appendFile(warehouse.writeRecords("file1s1.parquet",simpleTable.schema(),TestFixtures.FILE1SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file2s1.parquet",simpleTable.schema(),TestFixtures.FILE2SNAPSHOT1))
        .appendFile(warehouse.writeRecords("file3s1.parquet",simpleTable.schema(),TestFixtures.FILE3SNAPSHOT1))
        .commit();

  }

}
