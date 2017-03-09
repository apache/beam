package org.beam.sdk.java.sql.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@DefaultCoder(AvroCoder.class)
public class BeamSQLRecordType implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -5318734648766104712L;
  private List<String> fieldsName = new ArrayList<>();
  private List<String> fieldsType = new ArrayList<>();

  public static BeamSQLRecordType from(RelDataType tableInfo) {
    BeamSQLRecordType record = new BeamSQLRecordType();
    for (RelDataTypeField f : tableInfo.getFieldList()) {
      record.fieldsName.add(f.getName());
      record.fieldsType.add(f.getType().getSqlTypeName().getName());
    }
    return record;
  }

  public List<String> getFieldsName() {
    return fieldsName;
  }

  public void setFieldsName(List<String> fieldsName) {
    this.fieldsName = fieldsName;
  }

  public List<String> getFieldsType() {
    return fieldsType;
  }

  public void setFieldsType(List<String> fieldsType) {
    this.fieldsType = fieldsType;
  }

  @Override
  public String toString() {
    return "RecordType [fieldsName=" + fieldsName + ", fieldsType=" + fieldsType + "]";
  }

}
