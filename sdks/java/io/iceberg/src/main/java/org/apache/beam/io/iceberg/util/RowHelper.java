package org.apache.beam.io.iceberg.util;

import java.util.Optional;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.NestedField;

public class RowHelper {
  private RowHelper() { }

  public static void copyInto(GenericRecord rec,NestedField field,Row value) {
    String name = field.name();
    switch(field.type().typeId()) {
      case BOOLEAN:
        Optional.ofNullable(value.getBoolean(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case INTEGER:
        Optional.ofNullable(value.getInt32(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case LONG:
        Optional.ofNullable(value.getInt64(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case FLOAT:
        Optional.ofNullable(value.getFloat(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case DOUBLE:
        Optional.ofNullable(value.getDouble(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case DATE:
        break;
      case TIME:
        break;
      case TIMESTAMP:
        break;
      case STRING:
        Optional.ofNullable(value.getString(name)).ifPresent(v -> rec.setField(name,v));
        break;
      case UUID:
        break;
      case FIXED:
        break;
      case BINARY:
        break;
      case DECIMAL:
        break;
      case STRUCT:
        Optional.ofNullable(value.getRow(name))
            .ifPresent(row -> rec.setField(name,
                copy(GenericRecord.create(field.type().asStructType()),row)));
        break;
      case LIST:
        break;
      case MAP:
        break;
    }
  }

  public static Record copy(GenericRecord baseRecord, Row value) {
    GenericRecord rec = baseRecord.copy();
    for(NestedField f : rec.struct().fields()) {
      copyInto(rec,f,value);
    }
    return rec;
  }
}
