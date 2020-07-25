package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;

public class CEPMeasure implements Serializable {

  private final String outTableName;
  private final CEPOperation opr;
  private final CEPFieldRef fieldRef;
  private final Schema.FieldType fieldType;

  public CEPMeasure(Schema streamSchema, String outTableName, CEPOperation opr) {
    this.outTableName = outTableName;
    this.opr = opr;
    this.fieldRef = CEPUtil.getFieldRef(opr);
    this.fieldType = CEPUtil.getFieldType(streamSchema, opr);
  }

  // return the out column name
  public String getName() {
    return outTableName;
  }

  public CEPOperation getOperation() {
    return opr;
  }

  public CEPFieldRef getField() {
    return fieldRef;
  }

  public Schema.FieldType getType() {
    return fieldType;
  }
}
