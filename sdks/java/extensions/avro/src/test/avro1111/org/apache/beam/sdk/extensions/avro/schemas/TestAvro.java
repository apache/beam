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
package org.apache.beam.sdk.extensions.avro.schemas;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

@org.apache.avro.specific.AvroGenerated
public class TestAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 27902431178981259L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestAvro\",\"namespace\":\"org.apache.beam.sdk.extensions.avro.schemas\",\"fields\":[{\"name\":\"bool_non_nullable\",\"type\":\"boolean\"},{\"name\":\"int\",\"type\":[\"int\",\"null\"]},{\"name\":\"long\",\"type\":[\"long\",\"null\"]},{\"name\":\"float\",\"type\":[\"float\",\"null\"]},{\"name\":\"double\",\"type\":[\"double\",\"null\"]},{\"name\":\"string\",\"type\":[\"string\",\"null\"]},{\"name\":\"bytes\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"fixed\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed4\",\"size\":4}},{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"timestampMillis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"TestEnum\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"abc\",\"cde\"]}},{\"name\":\"row\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"TestAvroNested\",\"fields\":[{\"name\":\"BOOL_NON_NULLABLE\",\"type\":\"boolean\"},{\"name\":\"int\",\"type\":[\"int\",\"null\"]}]}]},{\"name\":\"array\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"TestAvroNested\"]}]},{\"name\":\"map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"TestAvroNested\"]}]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();
  static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
  }

  private static final BinaryMessageEncoder<TestAvro> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<TestAvro> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<TestAvro> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<TestAvro> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<TestAvro> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this TestAvro to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a TestAvro from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a TestAvro instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static TestAvro fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private boolean bool_non_nullable;
  private Integer int$;
  private Long long$;
  private Float float$;
  private Double double$;
  private CharSequence string;
  private java.nio.ByteBuffer bytes;
  private fixed4 fixed;
  private java.time.LocalDate date;
  private java.time.Instant timestampMillis;
  private org.apache.beam.sdk.extensions.avro.schemas.TestEnum TestEnum;
  private TestAvroNested row;
  private java.util.List<TestAvroNested> array;
  private java.util.Map<CharSequence, TestAvroNested> map;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public TestAvro() {}

  /**
   * All-args constructor.
   * @param bool_non_nullable The new value for bool_non_nullable
   * @param int$ The new value for int
   * @param long$ The new value for long
   * @param float$ The new value for float
   * @param double$ The new value for double
   * @param string The new value for string
   * @param bytes The new value for bytes
   * @param fixed The new value for fixed
   * @param date The new value for date
   * @param timestampMillis The new value for timestampMillis
   * @param TestEnum The new value for TestEnum
   * @param row The new value for row
   * @param array The new value for array
   * @param map The new value for map
   */
  public TestAvro(Boolean bool_non_nullable, Integer int$, Long long$, Float float$, Double double$, CharSequence string, java.nio.ByteBuffer bytes, fixed4 fixed, java.time.LocalDate date, java.time.Instant timestampMillis, org.apache.beam.sdk.extensions.avro.schemas.TestEnum TestEnum, TestAvroNested row, java.util.List<TestAvroNested> array, java.util.Map<CharSequence, TestAvroNested> map) {
    this.bool_non_nullable = bool_non_nullable;
    this.int$ = int$;
    this.long$ = long$;
    this.float$ = float$;
    this.double$ = double$;
    this.string = string;
    this.bytes = bytes;
    this.fixed = fixed;
    this.date = date;
    this.timestampMillis = timestampMillis.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    this.TestEnum = TestEnum;
    this.row = row;
    this.array = array;
    this.map = map;
  }

  /**
   * Manually added a ompatible with Avro v1.8.2 API constructor
   *
   * @param bool_non_nullable
   * @param int$
   * @param long$
   * @param float$
   * @param double$
   * @param string
   * @param bytes
   * @param fixed
   * @param date
   * @param timestampMillis
   * @param TestEnum
   * @param row
   * @param array
   * @param map
   */
  public TestAvro(java.lang.Boolean bool_non_nullable, java.lang.Integer int$, java.lang.Long long$, java.lang.Float float$, java.lang.Double double$, java.lang.String string, java.nio.ByteBuffer bytes, org.apache.beam.sdk.extensions.avro.schemas.fixed4 fixed, org.joda.time.LocalDate date, org.joda.time.DateTime timestampMillis, org.apache.beam.sdk.extensions.avro.schemas.TestEnum TestEnum, org.apache.beam.sdk.extensions.avro.schemas.TestAvroNested row, java.util.List<org.apache.beam.sdk.extensions.avro.schemas.TestAvroNested> array, java.util.Map<java.lang.String,org.apache.beam.sdk.extensions.avro.schemas.TestAvroNested> map) {
    this.bool_non_nullable = bool_non_nullable;
    this.int$ = int$;
    this.long$ = long$;
    this.float$ = float$;
    this.double$ = double$;
    this.string = string;
    this.bytes = bytes;
    this.fixed = fixed;
    this.date = LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
    this.timestampMillis = Instant.ofEpochMilli(timestampMillis.getMillis());
    this.TestEnum = TestEnum;
    this.row = row;
    this.array = array;
    this.map = (Map)map;
  }

  @Override
  public SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public Object get(int field$) {
    switch (field$) {
    case 0: return bool_non_nullable;
    case 1: return int$;
    case 2: return long$;
    case 3: return float$;
    case 4: return double$;
    case 5: return string;
    case 6: return bytes;
    case 7: return fixed;
    case 8: return date;
    case 9: return timestampMillis;
    case 10: return TestEnum;
    case 11: return row;
    case 12: return array;
    case 13: return map;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      new org.apache.avro.data.TimeConversions.DateConversion(),
      new org.apache.avro.data.TimeConversions.TimestampMillisConversion(),
      null,
      null,
      null,
      null,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: bool_non_nullable = (Boolean)value$; break;
    case 1: int$ = (Integer)value$; break;
    case 2: long$ = (Long)value$; break;
    case 3: float$ = (Float)value$; break;
    case 4: double$ = (Double)value$; break;
    case 5: string = (CharSequence)value$; break;
    case 6: bytes = (java.nio.ByteBuffer)value$; break;
    case 7: fixed = (fixed4)value$; break;
    case 8: date = (java.time.LocalDate)value$; break;
    case 9: timestampMillis = (java.time.Instant)value$; break;
    case 10: TestEnum = (org.apache.beam.sdk.extensions.avro.schemas.TestEnum)value$; break;
    case 11: row = (TestAvroNested)value$; break;
    case 12: array = (java.util.List<TestAvroNested>)value$; break;
    case 13: map = (java.util.Map<CharSequence, TestAvroNested>)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'bool_non_nullable' field.
   * @return The value of the 'bool_non_nullable' field.
   */
  public boolean getBoolNonNullable() {
    return bool_non_nullable;
  }


  /**
   * Sets the value of the 'bool_non_nullable' field.
   * @param value the value to set.
   */
  public void setBoolNonNullable(boolean value) {
    this.bool_non_nullable = value;
  }

  /**
   * Gets the value of the 'int$' field.
   * @return The value of the 'int$' field.
   */
  public Integer getInt$() {
    return int$;
  }


  /**
   * Sets the value of the 'int$' field.
   * @param value the value to set.
   */
  public void setInt$(Integer value) {
    this.int$ = value;
  }

  /**
   * Gets the value of the 'long$' field.
   * @return The value of the 'long$' field.
   */
  public Long getLong$() {
    return long$;
  }


  /**
   * Sets the value of the 'long$' field.
   * @param value the value to set.
   */
  public void setLong$(Long value) {
    this.long$ = value;
  }

  /**
   * Gets the value of the 'float$' field.
   * @return The value of the 'float$' field.
   */
  public Float getFloat$() {
    return float$;
  }


  /**
   * Sets the value of the 'float$' field.
   * @param value the value to set.
   */
  public void setFloat$(Float value) {
    this.float$ = value;
  }

  /**
   * Gets the value of the 'double$' field.
   * @return The value of the 'double$' field.
   */
  public Double getDouble$() {
    return double$;
  }


  /**
   * Sets the value of the 'double$' field.
   * @param value the value to set.
   */
  public void setDouble$(Double value) {
    this.double$ = value;
  }

  /**
   * Gets the value of the 'string' field.
   * @return The value of the 'string' field.
   */
  public CharSequence getString() {
    return string;
  }


  /**
   * Sets the value of the 'string' field.
   * @param value the value to set.
   */
  public void setString(CharSequence value) {
    this.string = value;
  }

  /**
   * Gets the value of the 'bytes' field.
   * @return The value of the 'bytes' field.
   */
  public java.nio.ByteBuffer getBytes() {
    return bytes;
  }


  /**
   * Sets the value of the 'bytes' field.
   * @param value the value to set.
   */
  public void setBytes(java.nio.ByteBuffer value) {
    this.bytes = value;
  }

  /**
   * Gets the value of the 'fixed' field.
   * @return The value of the 'fixed' field.
   */
  public fixed4 getFixed() {
    return fixed;
  }


  /**
   * Sets the value of the 'fixed' field.
   * @param value the value to set.
   */
  public void setFixed(fixed4 value) {
    this.fixed = value;
  }

  /**
   * Gets the value of the 'date' field.
   * @return The value of the 'date' field.
   */
  public java.time.LocalDate getDate() {
    return date;
  }


  /**
   * Sets the value of the 'date' field.
   * @param value the value to set.
   */
  public void setDate(java.time.LocalDate value) {
    this.date = value;
  }

  /**
   * Gets the value of the 'timestampMillis' field.
   * @return The value of the 'timestampMillis' field.
   */
  public java.time.Instant getTimestampMillis() {
    return timestampMillis;
  }


  /**
   * Sets the value of the 'timestampMillis' field.
   * @param value the value to set.
   */
  public void setTimestampMillis(java.time.Instant value) {
    this.timestampMillis = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
  }

  /**
   * Gets the value of the 'TestEnum' field.
   * @return The value of the 'TestEnum' field.
   */
  public org.apache.beam.sdk.extensions.avro.schemas.TestEnum getTestEnum() {
    return TestEnum;
  }


  /**
   * Sets the value of the 'TestEnum' field.
   * @param value the value to set.
   */
  public void setTestEnum(org.apache.beam.sdk.extensions.avro.schemas.TestEnum value) {
    this.TestEnum = value;
  }

  /**
   * Gets the value of the 'row' field.
   * @return The value of the 'row' field.
   */
  public TestAvroNested getRow() {
    return row;
  }


  /**
   * Sets the value of the 'row' field.
   * @param value the value to set.
   */
  public void setRow(TestAvroNested value) {
    this.row = value;
  }

  /**
   * Gets the value of the 'array' field.
   * @return The value of the 'array' field.
   */
  public java.util.List<TestAvroNested> getArray() {
    return array;
  }


  /**
   * Sets the value of the 'array' field.
   * @param value the value to set.
   */
  public void setArray(java.util.List<TestAvroNested> value) {
    this.array = value;
  }

  /**
   * Gets the value of the 'map' field.
   * @return The value of the 'map' field.
   */
  public java.util.Map<CharSequence, TestAvroNested> getMap() {
    return map;
  }


  /**
   * Sets the value of the 'map' field.
   * @param value the value to set.
   */
  public void setMap(java.util.Map<CharSequence, TestAvroNested> value) {
    this.map = value;
  }

  /**
   * Creates a new TestAvro RecordBuilder.
   * @return A new TestAvro RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new TestAvro RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new TestAvro RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new TestAvro RecordBuilder by copying an existing TestAvro instance.
   * @param other The existing instance to copy.
   * @return A new TestAvro RecordBuilder
   */
  public static Builder newBuilder(TestAvro other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for TestAvro instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TestAvro>
    implements org.apache.avro.data.RecordBuilder<TestAvro> {

    private boolean bool_non_nullable;
    private Integer int$;
    private Long long$;
    private Float float$;
    private Double double$;
    private CharSequence string;
    private java.nio.ByteBuffer bytes;
    private fixed4 fixed;
    private java.time.LocalDate date;
    private java.time.Instant timestampMillis;
    private org.apache.beam.sdk.extensions.avro.schemas.TestEnum TestEnum;
    private TestAvroNested row;
    private TestAvroNested.Builder rowBuilder;
    private java.util.List<TestAvroNested> array;
    private java.util.Map<CharSequence, TestAvroNested> map;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.bool_non_nullable)) {
        this.bool_non_nullable = data().deepCopy(fields()[0].schema(), other.bool_non_nullable);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.int$)) {
        this.int$ = data().deepCopy(fields()[1].schema(), other.int$);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.long$)) {
        this.long$ = data().deepCopy(fields()[2].schema(), other.long$);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.float$)) {
        this.float$ = data().deepCopy(fields()[3].schema(), other.float$);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.double$)) {
        this.double$ = data().deepCopy(fields()[4].schema(), other.double$);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.string)) {
        this.string = data().deepCopy(fields()[5].schema(), other.string);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
      if (isValidValue(fields()[6], other.bytes)) {
        this.bytes = data().deepCopy(fields()[6].schema(), other.bytes);
        fieldSetFlags()[6] = other.fieldSetFlags()[6];
      }
      if (isValidValue(fields()[7], other.fixed)) {
        this.fixed = data().deepCopy(fields()[7].schema(), other.fixed);
        fieldSetFlags()[7] = other.fieldSetFlags()[7];
      }
      if (isValidValue(fields()[8], other.date)) {
        this.date = data().deepCopy(fields()[8].schema(), other.date);
        fieldSetFlags()[8] = other.fieldSetFlags()[8];
      }
      if (isValidValue(fields()[9], other.timestampMillis)) {
        this.timestampMillis = data().deepCopy(fields()[9].schema(), other.timestampMillis);
        fieldSetFlags()[9] = other.fieldSetFlags()[9];
      }
      if (isValidValue(fields()[10], other.TestEnum)) {
        this.TestEnum = data().deepCopy(fields()[10].schema(), other.TestEnum);
        fieldSetFlags()[10] = other.fieldSetFlags()[10];
      }
      if (isValidValue(fields()[11], other.row)) {
        this.row = data().deepCopy(fields()[11].schema(), other.row);
        fieldSetFlags()[11] = other.fieldSetFlags()[11];
      }
      if (other.hasRowBuilder()) {
        this.rowBuilder = TestAvroNested.newBuilder(other.getRowBuilder());
      }
      if (isValidValue(fields()[12], other.array)) {
        this.array = data().deepCopy(fields()[12].schema(), other.array);
        fieldSetFlags()[12] = other.fieldSetFlags()[12];
      }
      if (isValidValue(fields()[13], other.map)) {
        this.map = data().deepCopy(fields()[13].schema(), other.map);
        fieldSetFlags()[13] = other.fieldSetFlags()[13];
      }
    }

    /**
     * Creates a Builder by copying an existing TestAvro instance
     * @param other The existing instance to copy.
     */
    private Builder(TestAvro other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.bool_non_nullable)) {
        this.bool_non_nullable = data().deepCopy(fields()[0].schema(), other.bool_non_nullable);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.int$)) {
        this.int$ = data().deepCopy(fields()[1].schema(), other.int$);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.long$)) {
        this.long$ = data().deepCopy(fields()[2].schema(), other.long$);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.float$)) {
        this.float$ = data().deepCopy(fields()[3].schema(), other.float$);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.double$)) {
        this.double$ = data().deepCopy(fields()[4].schema(), other.double$);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.string)) {
        this.string = data().deepCopy(fields()[5].schema(), other.string);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.bytes)) {
        this.bytes = data().deepCopy(fields()[6].schema(), other.bytes);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.fixed)) {
        this.fixed = data().deepCopy(fields()[7].schema(), other.fixed);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.date)) {
        this.date = data().deepCopy(fields()[8].schema(), other.date);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.timestampMillis)) {
        this.timestampMillis = data().deepCopy(fields()[9].schema(), other.timestampMillis);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.TestEnum)) {
        this.TestEnum = data().deepCopy(fields()[10].schema(), other.TestEnum);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.row)) {
        this.row = data().deepCopy(fields()[11].schema(), other.row);
        fieldSetFlags()[11] = true;
      }
      this.rowBuilder = null;
      if (isValidValue(fields()[12], other.array)) {
        this.array = data().deepCopy(fields()[12].schema(), other.array);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.map)) {
        this.map = data().deepCopy(fields()[13].schema(), other.map);
        fieldSetFlags()[13] = true;
      }
    }

    /**
      * Gets the value of the 'bool_non_nullable' field.
      * @return The value.
      */
    public boolean getBoolNonNullable() {
      return bool_non_nullable;
    }


    /**
      * Sets the value of the 'bool_non_nullable' field.
      * @param value The value of 'bool_non_nullable'.
      * @return This builder.
      */
    public Builder setBoolNonNullable(boolean value) {
      validate(fields()[0], value);
      this.bool_non_nullable = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'bool_non_nullable' field has been set.
      * @return True if the 'bool_non_nullable' field has been set, false otherwise.
      */
    public boolean hasBoolNonNullable() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'bool_non_nullable' field.
      * @return This builder.
      */
    public Builder clearBoolNonNullable() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'int$' field.
      * @return The value.
      */
    public Integer getInt$() {
      return int$;
    }


    /**
      * Sets the value of the 'int$' field.
      * @param value The value of 'int$'.
      * @return This builder.
      */
    public Builder setInt$(Integer value) {
      validate(fields()[1], value);
      this.int$ = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'int$' field has been set.
      * @return True if the 'int$' field has been set, false otherwise.
      */
    public boolean hasInt$() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'int$' field.
      * @return This builder.
      */
    public Builder clearInt$() {
      int$ = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'long$' field.
      * @return The value.
      */
    public Long getLong$() {
      return long$;
    }


    /**
      * Sets the value of the 'long$' field.
      * @param value The value of 'long$'.
      * @return This builder.
      */
    public Builder setLong$(Long value) {
      validate(fields()[2], value);
      this.long$ = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'long$' field has been set.
      * @return True if the 'long$' field has been set, false otherwise.
      */
    public boolean hasLong$() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'long$' field.
      * @return This builder.
      */
    public Builder clearLong$() {
      long$ = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'float$' field.
      * @return The value.
      */
    public Float getFloat$() {
      return float$;
    }


    /**
      * Sets the value of the 'float$' field.
      * @param value The value of 'float$'.
      * @return This builder.
      */
    public Builder setFloat$(Float value) {
      validate(fields()[3], value);
      this.float$ = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'float$' field has been set.
      * @return True if the 'float$' field has been set, false otherwise.
      */
    public boolean hasFloat$() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'float$' field.
      * @return This builder.
      */
    public Builder clearFloat$() {
      float$ = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'double$' field.
      * @return The value.
      */
    public Double getDouble$() {
      return double$;
    }


    /**
      * Sets the value of the 'double$' field.
      * @param value The value of 'double$'.
      * @return This builder.
      */
    public Builder setDouble$(Double value) {
      validate(fields()[4], value);
      this.double$ = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'double$' field has been set.
      * @return True if the 'double$' field has been set, false otherwise.
      */
    public boolean hasDouble$() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'double$' field.
      * @return This builder.
      */
    public Builder clearDouble$() {
      double$ = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'string' field.
      * @return The value.
      */
    public CharSequence getString() {
      return string;
    }


    /**
      * Sets the value of the 'string' field.
      * @param value The value of 'string'.
      * @return This builder.
      */
    public Builder setString(CharSequence value) {
      validate(fields()[5], value);
      this.string = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'string' field has been set.
      * @return True if the 'string' field has been set, false otherwise.
      */
    public boolean hasString() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'string' field.
      * @return This builder.
      */
    public Builder clearString() {
      string = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'bytes' field.
      * @return The value.
      */
    public java.nio.ByteBuffer getBytes() {
      return bytes;
    }


    /**
      * Sets the value of the 'bytes' field.
      * @param value The value of 'bytes'.
      * @return This builder.
      */
    public Builder setBytes(java.nio.ByteBuffer value) {
      validate(fields()[6], value);
      this.bytes = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'bytes' field has been set.
      * @return True if the 'bytes' field has been set, false otherwise.
      */
    public boolean hasBytes() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'bytes' field.
      * @return This builder.
      */
    public Builder clearBytes() {
      bytes = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'fixed' field.
      * @return The value.
      */
    public fixed4 getFixed() {
      return fixed;
    }


    /**
      * Sets the value of the 'fixed' field.
      * @param value The value of 'fixed'.
      * @return This builder.
      */
    public Builder setFixed(fixed4 value) {
      validate(fields()[7], value);
      this.fixed = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'fixed' field has been set.
      * @return True if the 'fixed' field has been set, false otherwise.
      */
    public boolean hasFixed() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'fixed' field.
      * @return This builder.
      */
    public Builder clearFixed() {
      fixed = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /**
      * Gets the value of the 'date' field.
      * @return The value.
      */
    public java.time.LocalDate getDate() {
      return date;
    }


    /**
      * Sets the value of the 'date' field.
      * @param value The value of 'date'.
      * @return This builder.
      */
    public Builder setDate(java.time.LocalDate value) {
      validate(fields()[8], value);
      this.date = value;
      fieldSetFlags()[8] = true;
      return this;
    }

    /**
      * Checks whether the 'date' field has been set.
      * @return True if the 'date' field has been set, false otherwise.
      */
    public boolean hasDate() {
      return fieldSetFlags()[8];
    }


    /**
      * Clears the value of the 'date' field.
      * @return This builder.
      */
    public Builder clearDate() {
      fieldSetFlags()[8] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestampMillis' field.
      * @return The value.
      */
    public java.time.Instant getTimestampMillis() {
      return timestampMillis;
    }


    /**
      * Sets the value of the 'timestampMillis' field.
      * @param value The value of 'timestampMillis'.
      * @return This builder.
      */
    public Builder setTimestampMillis(java.time.Instant value) {
      validate(fields()[9], value);
      this.timestampMillis = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
      fieldSetFlags()[9] = true;
      return this;
    }

    /**
      * Checks whether the 'timestampMillis' field has been set.
      * @return True if the 'timestampMillis' field has been set, false otherwise.
      */
    public boolean hasTimestampMillis() {
      return fieldSetFlags()[9];
    }


    /**
      * Clears the value of the 'timestampMillis' field.
      * @return This builder.
      */
    public Builder clearTimestampMillis() {
      fieldSetFlags()[9] = false;
      return this;
    }

    /**
      * Gets the value of the 'TestEnum' field.
      * @return The value.
      */
    public org.apache.beam.sdk.extensions.avro.schemas.TestEnum getTestEnum() {
      return TestEnum;
    }


    /**
      * Sets the value of the 'TestEnum' field.
      * @param value The value of 'TestEnum'.
      * @return This builder.
      */
    public Builder setTestEnum(org.apache.beam.sdk.extensions.avro.schemas.TestEnum value) {
      validate(fields()[10], value);
      this.TestEnum = value;
      fieldSetFlags()[10] = true;
      return this;
    }

    /**
      * Checks whether the 'TestEnum' field has been set.
      * @return True if the 'TestEnum' field has been set, false otherwise.
      */
    public boolean hasTestEnum() {
      return fieldSetFlags()[10];
    }


    /**
      * Clears the value of the 'TestEnum' field.
      * @return This builder.
      */
    public Builder clearTestEnum() {
      TestEnum = null;
      fieldSetFlags()[10] = false;
      return this;
    }

    /**
      * Gets the value of the 'row' field.
      * @return The value.
      */
    public TestAvroNested getRow() {
      return row;
    }


    /**
      * Sets the value of the 'row' field.
      * @param value The value of 'row'.
      * @return This builder.
      */
    public Builder setRow(TestAvroNested value) {
      validate(fields()[11], value);
      this.rowBuilder = null;
      this.row = value;
      fieldSetFlags()[11] = true;
      return this;
    }

    /**
      * Checks whether the 'row' field has been set.
      * @return True if the 'row' field has been set, false otherwise.
      */
    public boolean hasRow() {
      return fieldSetFlags()[11];
    }

    /**
     * Gets the Builder instance for the 'row' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public TestAvroNested.Builder getRowBuilder() {
      if (rowBuilder == null) {
        if (hasRow()) {
          setRowBuilder(TestAvroNested.newBuilder(row));
        } else {
          setRowBuilder(TestAvroNested.newBuilder());
        }
      }
      return rowBuilder;
    }

    /**
     * Sets the Builder instance for the 'row' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */

    public Builder setRowBuilder(TestAvroNested.Builder value) {
      clearRow();
      rowBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'row' field has an active Builder instance
     * @return True if the 'row' field has an active Builder instance
     */
    public boolean hasRowBuilder() {
      return rowBuilder != null;
    }

    /**
      * Clears the value of the 'row' field.
      * @return This builder.
      */
    public Builder clearRow() {
      row = null;
      rowBuilder = null;
      fieldSetFlags()[11] = false;
      return this;
    }

    /**
      * Gets the value of the 'array' field.
      * @return The value.
      */
    public java.util.List<TestAvroNested> getArray() {
      return array;
    }


    /**
      * Sets the value of the 'array' field.
      * @param value The value of 'array'.
      * @return This builder.
      */
    public Builder setArray(java.util.List<TestAvroNested> value) {
      validate(fields()[12], value);
      this.array = value;
      fieldSetFlags()[12] = true;
      return this;
    }

    /**
      * Checks whether the 'array' field has been set.
      * @return True if the 'array' field has been set, false otherwise.
      */
    public boolean hasArray() {
      return fieldSetFlags()[12];
    }


    /**
      * Clears the value of the 'array' field.
      * @return This builder.
      */
    public Builder clearArray() {
      array = null;
      fieldSetFlags()[12] = false;
      return this;
    }

    /**
      * Gets the value of the 'map' field.
      * @return The value.
      */
    public java.util.Map<CharSequence, TestAvroNested> getMap() {
      return map;
    }


    /**
      * Sets the value of the 'map' field.
      * @param value The value of 'map'.
      * @return This builder.
      */
    public Builder setMap(java.util.Map<CharSequence, TestAvroNested> value) {
      validate(fields()[13], value);
      this.map = value;
      fieldSetFlags()[13] = true;
      return this;
    }

    /**
      * Checks whether the 'map' field has been set.
      * @return True if the 'map' field has been set, false otherwise.
      */
    public boolean hasMap() {
      return fieldSetFlags()[13];
    }


    /**
      * Clears the value of the 'map' field.
      * @return This builder.
      */
    public Builder clearMap() {
      map = null;
      fieldSetFlags()[13] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TestAvro build() {
      try {
        TestAvro record = new TestAvro();
        record.bool_non_nullable = fieldSetFlags()[0] ? this.bool_non_nullable : (Boolean) defaultValue(fields()[0]);
        record.int$ = fieldSetFlags()[1] ? this.int$ : (Integer) defaultValue(fields()[1]);
        record.long$ = fieldSetFlags()[2] ? this.long$ : (Long) defaultValue(fields()[2]);
        record.float$ = fieldSetFlags()[3] ? this.float$ : (Float) defaultValue(fields()[3]);
        record.double$ = fieldSetFlags()[4] ? this.double$ : (Double) defaultValue(fields()[4]);
        record.string = fieldSetFlags()[5] ? this.string : (CharSequence) defaultValue(fields()[5]);
        record.bytes = fieldSetFlags()[6] ? this.bytes : (java.nio.ByteBuffer) defaultValue(fields()[6]);
        record.fixed = fieldSetFlags()[7] ? this.fixed : (fixed4) defaultValue(fields()[7]);
        record.date = fieldSetFlags()[8] ? this.date : (java.time.LocalDate) defaultValue(fields()[8]);
        record.timestampMillis = fieldSetFlags()[9] ? this.timestampMillis : (java.time.Instant) defaultValue(fields()[9]);
        record.TestEnum = fieldSetFlags()[10] ? this.TestEnum : (org.apache.beam.sdk.extensions.avro.schemas.TestEnum) defaultValue(fields()[10]);
        if (rowBuilder != null) {
          try {
            record.row = this.rowBuilder.build();
          } catch (org.apache.avro.AvroMissingFieldException e) {
            e.addParentField(record.getSchema().getField("row"));
            throw e;
          }
        } else {
          record.row = fieldSetFlags()[11] ? this.row : (TestAvroNested) defaultValue(fields()[11]);
        }
        record.array = fieldSetFlags()[12] ? this.array : (java.util.List<TestAvroNested>) defaultValue(fields()[12]);
        record.map = fieldSetFlags()[13] ? this.map : (java.util.Map<CharSequence, TestAvroNested>) defaultValue(fields()[13]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TestAvro>
    WRITER$ = (org.apache.avro.io.DatumWriter<TestAvro>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TestAvro>
    READER$ = (org.apache.avro.io.DatumReader<TestAvro>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










