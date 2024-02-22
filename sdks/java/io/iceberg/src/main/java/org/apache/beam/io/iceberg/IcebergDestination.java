package org.apache.beam.io.iceberg;

import java.io.Serializable;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.iceberg.Schema;

public class IcebergDestination implements Serializable {

  ResourceId resourceId;
  String table;
  Schema schema;
  Iceberg.WriteFormat writeFormat;

  public IcebergDestination(ResourceId resourceId,String table,Schema schema,Iceberg.WriteFormat writeFormat) {
    this.resourceId = resourceId;
    this.table = table;
    this.schema = schema;
    this.writeFormat = writeFormat;
  }

  public Iceberg.WriteFormat getWriteFormat() {
    return writeFormat;
  }

  public Schema getSchema() {
    return schema;
  }
}
