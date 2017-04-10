package org.beam.dsls.sql.schema;

import org.apache.calcite.sql.type.SqlTypeName;

public class UnsupportedDataTypeException extends RuntimeException {

  public UnsupportedDataTypeException(SqlTypeName unsupportedType){
    super(String.format("Not support data type [%s]", unsupportedType));
  }

}
