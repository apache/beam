package org.apache.beam.io.iceberg;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
public class TestDataWarehouse extends ExternalResource {
  private static final Logger LOG = Logger.getLogger(TestDataWarehouse.class);

  protected final TemporaryFolder temporaryFolder;
  protected final String database;

  protected final Configuration hadoopConf;


  protected String location;
  protected Catalog catalog;


  public TestDataWarehouse(TemporaryFolder temporaryFolder,String database) {
    this.temporaryFolder = temporaryFolder;
    this.database = database;
    this.hadoopConf = new Configuration();
  }

  @Override
  protected void before() throws Throwable {
    File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    location = "file:"+warehouseFile.toString();
    catalog = CatalogUtil.loadCatalog(
        CatalogUtil.ICEBERG_CATALOG_HADOOP,"hadoop",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION,location),hadoopConf);
  }

  @Override
  protected void after() {
      List<TableIdentifier> tables = catalog.listTables(Namespace.of(database));
      LOG.info("Cleaning up "+ tables.size()+" tables");
      for(TableIdentifier t : tables) {
        try {
          catalog.dropTable(t);
        } catch(Exception e) {

        }
      }
      try {
        ((HadoopCatalog) catalog).close();
      } catch(Exception e) {
        LOG.error("Unable to close catalog",e);
      }
  }

  public DataFile writeRecords(String filename,Schema schema,List<Record> records) throws IOException {
    Path path = new Path(location,filename);
    FileFormat format = FileFormat.fromFileName(filename);


    FileAppender<Record> appender;
    switch (format) {
      case PARQUET:
        appender = Parquet.write(fromPath(path,hadoopConf))
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .schema(schema)
            .overwrite().build();
        break;
      case ORC:
        appender = ORC.write(fromPath(path,hadoopConf))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(schema)
            .overwrite().build();
        break;
      default:
        throw new IOException("Unable to create appender for "+format);
    }
    appender.addAll(records);
    appender.close();
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(HadoopInputFile.fromPath(path,hadoopConf))
        .withMetrics(appender.metrics())
        .build();
  }

  public Table createTable(Schema schema) {
    TableIdentifier table = TableIdentifier.of(database,"table"+Integer.toString(UUID.randomUUID().hashCode(),16));
    return catalog.createTable(table,schema);
  }


}
