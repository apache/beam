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

@org.apache.avro.specific.AvroGenerated
public class TestAvroNested extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 4633138088036298925L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestAvroNested\",\"namespace\":\"org.apache.beam.sdk.extensions.avro.schemas\",\"fields\":[{\"name\":\"BOOL_NON_NULLABLE\",\"type\":\"boolean\"},{\"name\":\"int\",\"type\":[\"int\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<TestAvroNested> ENCODER =
      new BinaryMessageEncoder<TestAvroNested>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<TestAvroNested> DECODER =
      new BinaryMessageDecoder<TestAvroNested>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<TestAvroNested> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<TestAvroNested> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<TestAvroNested> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<TestAvroNested>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this TestAvroNested to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a TestAvroNested from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a TestAvroNested instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static TestAvroNested fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private boolean BOOL_NON_NULLABLE;
   private Integer int$;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public TestAvroNested() {}

  /**
   * All-args constructor.
   * @param BOOL_NON_NULLABLE The new value for BOOL_NON_NULLABLE
   * @param int$ The new value for int
   */
  public TestAvroNested(Boolean BOOL_NON_NULLABLE, Integer int$) {
    this.BOOL_NON_NULLABLE = BOOL_NON_NULLABLE;
    this.int$ = int$;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return BOOL_NON_NULLABLE;
    case 1: return int$;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: BOOL_NON_NULLABLE = (Boolean)value$; break;
    case 1: int$ = (Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'BOOL_NON_NULLABLE' field.
   * @return The value of the 'BOOL_NON_NULLABLE' field.
   */
  public boolean getBOOLNONNULLABLE() {
    return BOOL_NON_NULLABLE;
  }


  /**
   * Sets the value of the 'BOOL_NON_NULLABLE' field.
   * @param value the value to set.
   */
  public void setBOOLNONNULLABLE(boolean value) {
    this.BOOL_NON_NULLABLE = value;
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
   * Creates a new TestAvroNested RecordBuilder.
   * @return A new TestAvroNested RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new TestAvroNested RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new TestAvroNested RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new TestAvroNested RecordBuilder by copying an existing TestAvroNested instance.
   * @param other The existing instance to copy.
   * @return A new TestAvroNested RecordBuilder
   */
  public static Builder newBuilder(TestAvroNested other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for TestAvroNested instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TestAvroNested>
    implements org.apache.avro.data.RecordBuilder<TestAvroNested> {

    private boolean BOOL_NON_NULLABLE;
    private Integer int$;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.BOOL_NON_NULLABLE)) {
        this.BOOL_NON_NULLABLE = data().deepCopy(fields()[0].schema(), other.BOOL_NON_NULLABLE);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.int$)) {
        this.int$ = data().deepCopy(fields()[1].schema(), other.int$);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing TestAvroNested instance
     * @param other The existing instance to copy.
     */
    private Builder(TestAvroNested other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.BOOL_NON_NULLABLE)) {
        this.BOOL_NON_NULLABLE = data().deepCopy(fields()[0].schema(), other.BOOL_NON_NULLABLE);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.int$)) {
        this.int$ = data().deepCopy(fields()[1].schema(), other.int$);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'BOOL_NON_NULLABLE' field.
      * @return The value.
      */
    public boolean getBOOLNONNULLABLE() {
      return BOOL_NON_NULLABLE;
    }


    /**
      * Sets the value of the 'BOOL_NON_NULLABLE' field.
      * @param value The value of 'BOOL_NON_NULLABLE'.
      * @return This builder.
      */
    public Builder setBOOLNONNULLABLE(boolean value) {
      validate(fields()[0], value);
      this.BOOL_NON_NULLABLE = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'BOOL_NON_NULLABLE' field has been set.
      * @return True if the 'BOOL_NON_NULLABLE' field has been set, false otherwise.
      */
    public boolean hasBOOLNONNULLABLE() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'BOOL_NON_NULLABLE' field.
      * @return This builder.
      */
    public Builder clearBOOLNONNULLABLE() {
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

    @Override
    @SuppressWarnings("unchecked")
    public TestAvroNested build() {
      try {
        TestAvroNested record = new TestAvroNested();
        record.BOOL_NON_NULLABLE = fieldSetFlags()[0] ? this.BOOL_NON_NULLABLE : (Boolean) defaultValue(fields()[0]);
        record.int$ = fieldSetFlags()[1] ? this.int$ : (Integer) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TestAvroNested>
    WRITER$ = (org.apache.avro.io.DatumWriter<TestAvroNested>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TestAvroNested>
    READER$ = (org.apache.avro.io.DatumReader<TestAvroNested>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeBoolean(this.BOOL_NON_NULLABLE);

    if (this.int$ == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeInt(this.int$);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.BOOL_NON_NULLABLE = in.readBoolean();

      if (in.readIndex() != 0) {
        in.readNull();
        this.int$ = null;
      } else {
        this.int$ = in.readInt();
      }

    } else {
      for (int i = 0; i < 2; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.BOOL_NON_NULLABLE = in.readBoolean();
          break;

        case 1:
          if (in.readIndex() != 0) {
            in.readNull();
            this.int$ = null;
          } else {
            this.int$ = in.readInt();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










