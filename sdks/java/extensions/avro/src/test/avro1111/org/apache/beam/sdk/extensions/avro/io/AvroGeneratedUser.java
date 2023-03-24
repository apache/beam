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
package org.apache.beam.sdk.extensions.avro.io;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class AvroGeneratedUser extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3056733241446362297L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroGeneratedUser\",\"namespace\":\"org.apache.beam.sdk.extensions.avro.io\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<AvroGeneratedUser> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<AvroGeneratedUser> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<AvroGeneratedUser> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<AvroGeneratedUser> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<AvroGeneratedUser> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this AvroGeneratedUser to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a AvroGeneratedUser from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a AvroGeneratedUser instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static AvroGeneratedUser fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.CharSequence name;
  private java.lang.Integer favorite_number;
  private java.lang.CharSequence favorite_color;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public AvroGeneratedUser() {}

  /**
   * All-args constructor.
   * @param name The new value for name
   * @param favorite_number The new value for favorite_number
   * @param favorite_color The new value for favorite_color
   */
  public AvroGeneratedUser(java.lang.CharSequence name, java.lang.Integer favorite_number, java.lang.CharSequence favorite_color) {
    this.name = name;
    this.favorite_number = favorite_number;
    this.favorite_color = favorite_color;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return favorite_number;
    case 2: return favorite_color;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.CharSequence)value$; break;
    case 1: favorite_number = (java.lang.Integer)value$; break;
    case 2: favorite_color = (java.lang.CharSequence)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }


  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'favorite_number' field.
   * @return The value of the 'favorite_number' field.
   */
  public java.lang.Integer getFavoriteNumber() {
    return favorite_number;
  }


  /**
   * Sets the value of the 'favorite_number' field.
   * @param value the value to set.
   */
  public void setFavoriteNumber(java.lang.Integer value) {
    this.favorite_number = value;
  }

  /**
   * Gets the value of the 'favorite_color' field.
   * @return The value of the 'favorite_color' field.
   */
  public java.lang.CharSequence getFavoriteColor() {
    return favorite_color;
  }


  /**
   * Sets the value of the 'favorite_color' field.
   * @param value the value to set.
   */
  public void setFavoriteColor(java.lang.CharSequence value) {
    this.favorite_color = value;
  }

  /**
   * Creates a new AvroGeneratedUser RecordBuilder.
   * @return A new AvroGeneratedUser RecordBuilder
   */
  public static org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder newBuilder() {
    return new org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder();
  }

  /**
   * Creates a new AvroGeneratedUser RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new AvroGeneratedUser RecordBuilder
   */
  public static org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder newBuilder(org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder other) {
    if (other == null) {
      return new org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder();
    } else {
      return new org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder(other);
    }
  }

  /**
   * Creates a new AvroGeneratedUser RecordBuilder by copying an existing AvroGeneratedUser instance.
   * @param other The existing instance to copy.
   * @return A new AvroGeneratedUser RecordBuilder
   */
  public static org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder newBuilder(org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser other) {
    if (other == null) {
      return new org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder();
    } else {
      return new org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder(other);
    }
  }

  /**
   * RecordBuilder for AvroGeneratedUser instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AvroGeneratedUser>
    implements org.apache.avro.data.RecordBuilder<AvroGeneratedUser> {

    private java.lang.CharSequence name;
    private java.lang.Integer favorite_number;
    private java.lang.CharSequence favorite_color;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.favorite_number)) {
        this.favorite_number = data().deepCopy(fields()[1].schema(), other.favorite_number);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.favorite_color)) {
        this.favorite_color = data().deepCopy(fields()[2].schema(), other.favorite_color);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing AvroGeneratedUser instance
     * @param other The existing instance to copy.
     */
    private Builder(org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.favorite_number)) {
        this.favorite_number = data().deepCopy(fields()[1].schema(), other.favorite_number);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.favorite_color)) {
        this.favorite_color = data().deepCopy(fields()[2].schema(), other.favorite_color);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return name;
    }


    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder setName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'favorite_number' field.
      * @return The value.
      */
    public java.lang.Integer getFavoriteNumber() {
      return favorite_number;
    }


    /**
      * Sets the value of the 'favorite_number' field.
      * @param value The value of 'favorite_number'.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder setFavoriteNumber(java.lang.Integer value) {
      validate(fields()[1], value);
      this.favorite_number = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'favorite_number' field has been set.
      * @return True if the 'favorite_number' field has been set, false otherwise.
      */
    public boolean hasFavoriteNumber() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'favorite_number' field.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder clearFavoriteNumber() {
      favorite_number = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'favorite_color' field.
      * @return The value.
      */
    public java.lang.CharSequence getFavoriteColor() {
      return favorite_color;
    }


    /**
      * Sets the value of the 'favorite_color' field.
      * @param value The value of 'favorite_color'.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder setFavoriteColor(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.favorite_color = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'favorite_color' field has been set.
      * @return True if the 'favorite_color' field has been set, false otherwise.
      */
    public boolean hasFavoriteColor() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'favorite_color' field.
      * @return This builder.
      */
    public org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser.Builder clearFavoriteColor() {
      favorite_color = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AvroGeneratedUser build() {
      try {
        AvroGeneratedUser record = new AvroGeneratedUser();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.favorite_number = fieldSetFlags()[1] ? this.favorite_number : (java.lang.Integer) defaultValue(fields()[1]);
        record.favorite_color = fieldSetFlags()[2] ? this.favorite_color : (java.lang.CharSequence) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<AvroGeneratedUser>
    WRITER$ = (org.apache.avro.io.DatumWriter<AvroGeneratedUser>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<AvroGeneratedUser>
    READER$ = (org.apache.avro.io.DatumReader<AvroGeneratedUser>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.name);

    if (this.favorite_number == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeInt(this.favorite_number);
    }

    if (this.favorite_color == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeString(this.favorite_color);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);

      if (in.readIndex() != 0) {
        in.readNull();
        this.favorite_number = null;
      } else {
        this.favorite_number = in.readInt();
      }

      if (in.readIndex() != 0) {
        in.readNull();
        this.favorite_color = null;
      } else {
        this.favorite_color = in.readString(this.favorite_color instanceof Utf8 ? (Utf8)this.favorite_color : null);
      }

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);
          break;

        case 1:
          if (in.readIndex() != 0) {
            in.readNull();
            this.favorite_number = null;
          } else {
            this.favorite_number = in.readInt();
          }
          break;

        case 2:
          if (in.readIndex() != 0) {
            in.readNull();
            this.favorite_color = null;
          } else {
            this.favorite_color = in.readString(this.favorite_color instanceof Utf8 ? (Utf8)this.favorite_color : null);
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










