package org.apache.beam.io.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
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
public class SinkTests {
  private static Logger LOG = LoggerFactory.getLogger(SinkTests.class);
  @ClassRule
  public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestDataWarehouse warehouse = new TestDataWarehouse(temporaryFolder,"default");

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();


  @Test
  public void testSimpleAppend() throws Exception {
    //Create a table and add records to it.
    Table table = warehouse.createTable(TestFixtures.SCHEMA);

    Iceberg.Catalog catalog = Iceberg.Catalog.builder()
        .name("hadoop")
        .icebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
        .warehouseLocation(warehouse.location)
        .build();

    String[] tablePath = table.name()
        .replace("hadoop.","").split("\\.");
   DynamicDestinations<Row,String> destination = DynamicDestinations.constant(catalog.table()
       .tablePath(ImmutableList.copyOf(tablePath)).build());
    LOG.info("Table created. Making pipeline");
    testPipeline
        .apply("Records To Add",
            Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .apply("Append To Table",
            new Iceberg.Write(
                catalog,
                destination));
    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");
  }

}
