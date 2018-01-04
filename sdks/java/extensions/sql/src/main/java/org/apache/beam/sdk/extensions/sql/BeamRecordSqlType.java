/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.BooleanCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DateCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DoubleCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.FloatCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.ShortCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.TimeCoder;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;

/**
 * Type provider for {@link BeamRecord} with SQL types.
 *
 * <p>Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/blog/2017/07/21/sql-dsl.html#data-type">data types</a>
 * for more details.
 *
 */
public class BeamRecordSqlType extends BeamRecordType {
  private static final Map<Integer, Class> JAVA_CLASSES = ImmutableMap
      .<Integer, Class>builder()
      .put(Types.TINYINT, Byte.class)
      .put(Types.SMALLINT, Short.class)
      .put(Types.INTEGER, Integer.class)
      .put(Types.BIGINT, Long.class)
      .put(Types.FLOAT, Float.class)
      .put(Types.DOUBLE, Double.class)
      .put(Types.DECIMAL, BigDecimal.class)
      .put(Types.BOOLEAN, Boolean.class)
      .put(Types.CHAR, String.class)
      .put(Types.VARCHAR, String.class)
      .put(Types.TIME, GregorianCalendar.class)
      .put(Types.DATE, Date.class)
      .put(Types.TIMESTAMP, Date.class)
      .build();

  private static final Map<Integer, Coder> CODERS = ImmutableMap
      .<Integer, Coder>builder()
      .put(Types.TINYINT, ByteCoder.of())
      .put(Types.SMALLINT, ShortCoder.of())
      .put(Types.INTEGER, BigEndianIntegerCoder.of())
      .put(Types.BIGINT, BigEndianLongCoder.of())
      .put(Types.FLOAT, FloatCoder.of())
      .put(Types.DOUBLE, DoubleCoder.of())
      .put(Types.DECIMAL, BigDecimalCoder.of())
      .put(Types.BOOLEAN, BooleanCoder.of())
      .put(Types.CHAR, StringUtf8Coder.of())
      .put(Types.VARCHAR, StringUtf8Coder.of())
      .put(Types.TIME, TimeCoder.of())
      .put(Types.DATE, DateCoder.of())
      .put(Types.TIMESTAMP, DateCoder.of())
      .build();

  public List<Integer> fieldTypes;

  protected BeamRecordSqlType(List<String> fieldsName, List<Coder> fieldsCoder) {
    super(fieldsName, fieldsCoder);
  }

  private BeamRecordSqlType(List<String> fieldsName, List<Integer> fieldTypes
      , List<Coder> fieldsCoder) {
    super(fieldsName, fieldsCoder);
    this.fieldTypes = fieldTypes;
  }

  public static BeamRecordSqlType create(List<String> fieldNames,
                                         List<Integer> fieldTypes) {
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalStateException("the sizes of 'dataType' and 'fieldTypes' must match.");
    }

    List<Coder> fieldCoders = new ArrayList<>(fieldTypes.size());

    for (int idx = 0; idx < fieldTypes.size(); ++idx) {
      Integer fieldType = fieldTypes.get(idx);

      if (!CODERS.containsKey(fieldType)) {
        throw new UnsupportedOperationException(
            "Data type: " + fieldType + " not supported yet!");
      }

      fieldCoders.add(CODERS.get(fieldType));
    }

    return new BeamRecordSqlType(fieldNames, fieldTypes, fieldCoders);
  }

  @Override
  public void validateValueType(int index, Object fieldValue) throws IllegalArgumentException {
    if (null == fieldValue) {// no need to do type check for NULL value
      return;
    }

    int fieldType = fieldTypes.get(index);
    Class javaClazz = JAVA_CLASSES.get(fieldType);
    if (javaClazz == null) {
      throw new IllegalArgumentException("Data type: " + fieldType + " not supported yet!");
    }

    if (!fieldValue.getClass().equals(javaClazz)) {
      throw new IllegalArgumentException(
          String.format("[%s](%s) doesn't match type [%s]",
              fieldValue, fieldValue.getClass(), fieldType)
      );
    }
  }

  public List<Integer> getFieldTypes() {
    return Collections.unmodifiableList(fieldTypes);
  }

  public Integer getFieldTypeByIndex(int index) {
    return fieldTypes.get(index);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof BeamRecordSqlType) {
      BeamRecordSqlType ins = (BeamRecordSqlType) obj;
      return fieldTypes.equals(ins.getFieldTypes()) && getFieldNames().equals(ins.getFieldNames());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * getFieldNames().hashCode() + getFieldTypes().hashCode();
  }

  @Override
  public String toString() {
    return "BeamRecordSqlType [fieldNames=" + getFieldNames()
        + ", fieldTypes=" + fieldTypes + "]";
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to construct {@link BeamRecordSqlType}.
   */
  public static class Builder {

    private ImmutableList.Builder<String> fieldNames;
    private ImmutableList.Builder<Integer> fieldTypes;

    public Builder withField(String fieldName, Integer fieldType) {
      fieldNames.add(fieldName);
      fieldTypes.add(fieldType);
      return this;
    }

    public Builder withTinyIntField(String fieldName) {
      return withField(fieldName, Types.TINYINT);
    }

    public Builder withSmallIntField(String fieldName) {
      return withField(fieldName, Types.SMALLINT);
    }

    public Builder withIntegerField(String fieldName) {
      return withField(fieldName, Types.INTEGER);
    }

    public Builder withBigIntField(String fieldName) {
      return withField(fieldName, Types.BIGINT);
    }

    public Builder withFloatField(String fieldName) {
      return withField(fieldName, Types.FLOAT);
    }

    public Builder withDoubleField(String fieldName) {
      return withField(fieldName, Types.DOUBLE);
    }

    public Builder withDecimalField(String fieldName) {
      return withField(fieldName, Types.DECIMAL);
    }

    public Builder withBooleanField(String fieldName) {
      return withField(fieldName, Types.BOOLEAN);
    }

    public Builder withCharField(String fieldName) {
      return withField(fieldName, Types.CHAR);
    }

    public Builder withVarcharField(String fieldName) {
      return withField(fieldName, Types.VARCHAR);
    }

    public Builder withTimeField(String fieldName) {
      return withField(fieldName, Types.TIME);
    }

    public Builder withDateField(String fieldName) {
      return withField(fieldName, Types.DATE);
    }

    public Builder withTimestampField(String fieldName) {
      return withField(fieldName, Types.TIMESTAMP);
    }

    private Builder() {
      this.fieldNames = ImmutableList.builder();
      this.fieldTypes = ImmutableList.builder();
    }

    public BeamRecordSqlType build() {
      return create(fieldNames.build(), fieldTypes.build());
    }
  }
}
