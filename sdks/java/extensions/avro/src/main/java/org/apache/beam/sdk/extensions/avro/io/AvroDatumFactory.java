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

import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Create {@link DatumReader} and {@link DatumWriter} for given schemas. */
public abstract class AvroDatumFactory<T>
    implements AvroSource.DatumReaderFactory<T>, AvroSink.DatumWriterFactory<T> {

  /** Returns an {@link AvroDatumFactory} instance for GenericRecord. */
  public static AvroDatumFactory<GenericRecord> generic() {
    return GenericDatumFactory.INSTANCE;
  }

  /**
   * Returns an {@link AvroDatumFactory} instance for the provided element type respecting Avro's
   * Specific* suite for encoding and decoding.
   */
  public static <T> AvroDatumFactory<T> specific(Class<T> type) {
    return new SpecificDatumFactory<>(type);
  }

  /**
   * Returns an {@link AvroDatumFactory} instance for the provided element type respecting Avro's
   * Reflect* suite for encoding and decoding.
   */
  public static <T> AvroDatumFactory<T> reflect(Class<T> type) {
    return new ReflectDatumFactory<>(type);
  }

  /** Returns an {@link AvroDatumFactory} instance for the provided element type. */
  public static <T> AvroDatumFactory<T> of(Class<T> type) {
    return of(type, true);
  }

  /**
   * Returns an {@link AvroDatumFactory} instance for the provided element type respecting Avro's
   * Reflect* or Specific* suite for encoding and decoding.
   */
  public static <T> AvroDatumFactory<T> of(Class<T> type, boolean useReflectApi) {
    if (GenericRecord.class.equals(type)) {
      return (AvroDatumFactory<T>) AvroDatumFactory.GenericDatumFactory.INSTANCE;
    } else if (SpecificRecord.class.isAssignableFrom(type) && !useReflectApi) {
      return new AvroDatumFactory.SpecificDatumFactory<>(type);
    } else {
      return new AvroDatumFactory.ReflectDatumFactory<>(type);
    }
  }

  protected final Class<T> type;

  public AvroDatumFactory(Class<T> type) {
    this.type = type;
  }

  /** Returns the type for the datum factory. */
  public Class<T> getType() {
    return type;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    AvroDatumFactory<?> that = (AvroDatumFactory<?>) other;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), type);
  }

  /** Specialized {@link AvroDatumFactory} for {@link GenericRecord}. */
  public static class GenericDatumFactory extends AvroDatumFactory<GenericRecord> {

    public static final GenericDatumFactory INSTANCE = new GenericDatumFactory();

    public GenericDatumFactory() {
      super(GenericRecord.class);
    }

    @Override
    public DatumReader<GenericRecord> apply(Schema writer, Schema reader) {
      return new GenericDatumReader<>(writer, reader);
    }

    @Override
    public DatumWriter<GenericRecord> apply(Schema writer) {
      return new GenericDatumWriter<>(writer);
    }
  }

  /** Specialized {@link AvroDatumFactory} for {@link org.apache.avro.specific.SpecificRecord}. */
  public static class SpecificDatumFactory<T> extends AvroDatumFactory<T> {
    public SpecificDatumFactory(Class<T> type) {
      super(type);
    }

    @Override
    public DatumReader<T> apply(Schema writer, Schema reader) {
      // create the datum writer using the Class<T> api.
      // avro will load the proper class loader and when using avro 1.9
      // the proper data with conversions (SpecificData.getForClass)
      SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(this.type);
      datumReader.setExpected(reader);
      datumReader.setSchema(writer);
      // logical types should be added to the data's model by the specific compiler
      return datumReader;
    }

    @Override
    public DatumWriter<T> apply(Schema writer) {
      // create the datum writer using the Class<T> api.
      // avro will load the proper class loader and when using avro 1.9
      // the proper data with conversions (SpecificData.getForClass)
      SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(type);
      datumWriter.setSchema(writer);
      // logical types should be added to the data's model by the specific compiler
      return datumWriter;
    }

    public static <T> SpecificDatumFactory<T> of(Class<T> type) {
      return new SpecificDatumFactory<>(type);
    }
  }

  /**
   * Specialized {@link AvroDatumFactory} for java classes transforming to avro through reflection.
   */
  public static class ReflectDatumFactory<T> extends AvroDatumFactory<T> {
    public ReflectDatumFactory(Class<T> type) {
      super(type);
    }

    @Override
    public DatumReader<T> apply(Schema writer, Schema reader) {
      // create the datum writer using the Class<T> api.
      // avro will load the proper class loader
      ReflectDatumReader<T> datumReader = new ReflectDatumReader<>(type);
      datumReader.setExpected(reader);
      datumReader.setSchema(writer);
      // for backward compat, add logical type support by default
      AvroUtils.addLogicalTypeConversions(datumReader.getData());
      return datumReader;
    }

    @Override
    public DatumWriter<T> apply(Schema writer) {
      // create the datum writer using the Class<T> api.
      // avro will load the proper class loader
      ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(type);
      datumWriter.setSchema(writer);
      // for backward compat, add logical type support by default
      AvroUtils.addLogicalTypeConversions(datumWriter.getData());
      return datumWriter;
    }

    public static <T> ReflectDatumFactory<T> of(Class<T> type) {
      return new ReflectDatumFactory<>(type);
    }
  }
}
