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
package org.apache.beam.sdk.io.csv;

import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;

class CsvIOTestHelpers {
  private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER = new DefaultSchemaProvider();
  static final Schema ALL_DATA_TYPES_SCHEMA =
      DEFAULT_SCHEMA_PROVIDER.schemaFor(TypeDescriptor.of(AllDataTypes.class));

  static Row rowOf(
      boolean aBoolean,
      byte aByte,
      DateTime dateTime,
      BigDecimal decimal,
      double aDouble,
      float aFloat,
      short aShort,
      int anInt,
      long aLong,
      String string) {
    return new AllDataTypes(
            aBoolean, aByte, dateTime, decimal, aDouble, aFloat, aShort, anInt, aLong, string)
        .toRow();
  }

  @DefaultSchema(JavaBeanSchema.class)
  static class AllDataTypes {

    static AllDataTypes of(
        boolean aBoolean,
        byte aByte,
        DateTime dateTime,
        BigDecimal decimal,
        double aDouble,
        float aFloat,
        short aShort,
        int anInt,
        long aLong,
        String string) {
      return new AllDataTypes(
          aBoolean, aByte, dateTime, decimal, aDouble, aFloat, aShort, anInt, aLong, string);
    }

    private final boolean aBoolean;
    private final byte aByte;
    private final DateTime dateTime;
    private final BigDecimal decimal;
    private final double aDouble;
    private final float aFloat;
    private final short aShort;
    private final int anInt;
    private final long aLong;
    private final String string;

    @SchemaCreate
    AllDataTypes(
        boolean aBoolean,
        byte aByte,
        DateTime dateTime,
        BigDecimal decimal,
        double aDouble,
        float aFloat,
        short aShort,
        int anInt,
        long aLong,
        String string) {
      this.aBoolean = aBoolean;
      this.aByte = aByte;
      this.dateTime = dateTime;
      this.decimal = decimal;
      this.aDouble = aDouble;
      this.aFloat = aFloat;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.string = string;
    }

    Row toRow() {
      return DEFAULT_SCHEMA_PROVIDER
          .toRowFunction(TypeDescriptor.of(AllDataTypes.class))
          .apply(this);
    }

    public boolean isaBoolean() {
      return aBoolean;
    }

    public byte getaByte() {
      return aByte;
    }

    public DateTime getDateTime() {
      return dateTime;
    }

    public BigDecimal getDecimal() {
      return decimal;
    }

    public double getaDouble() {
      return aDouble;
    }

    public float getaFloat() {
      return aFloat;
    }

    public short getaShort() {
      return aShort;
    }

    public int getAnInt() {
      return anInt;
    }

    public long getaLong() {
      return aLong;
    }

    public String getString() {
      return string;
    }
  }
}
