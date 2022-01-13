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
package org.apache.beam.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;

/** A {@link CustomCoder} using Avro specific format. */
public class AvroSpecificCoder<T extends SpecificRecord> extends CustomCoder<T> {

  /**
   * Returns an {@code AvroSpecificCoder} instance for the provided element type. The type must be
   * auto-generated avro class.
   *
   * @param <T> the element type
   */
  public static <T extends SpecificRecord> AvroSpecificCoder<T> of(Class<T> type) {
    try {
      Schema schema = (Schema) type.getField("SCHEMA$").get(null);
      return of(type, schema);
    } catch (Exception e) {
      throw new RuntimeException("Unable to get schema from " + type);
    }
  }

  /**
   * Returns an {@code AvroSpecificCoder} instance for the provided element type using the provided
   * Avro schema.
   *
   * <p>The schema must correspond to the type provided.
   *
   * @param <T> the element type
   * @param schema the avro schema
   */
  public static <T extends SpecificRecord> AvroSpecificCoder<T> of(Class<T> type, Schema schema) {
    return new AvroSpecificCoder<>(type, schema);
  }

  @SuppressWarnings("unused")
  private final Class<T> type;
  private final SerializableSchemaSupplier schemaSupplier;
  private final TypeDescriptor<T> typeDescriptor;

  // Factories allocated by .get() are thread-safe and immutable.
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  /**
   * A {@link Serializable} object that holds the {@link String} version of a {@link Schema}. This
   * is paired with the {@link SerializableSchemaSupplier} via {@link Serializable}'s usage of the
   * {@link #readResolve} method.
   */
  private static class SerializableSchemaString implements Serializable {
    private final String schema;

    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(new Schema.Parser().parse(schema));
    }
  }

  /**
   * A {@link Serializable} object that delegates to the {@link SerializableSchemaString} via {@link
   * Serializable}'s usage of the {@link #writeReplace} method. Kryo doesn't utilize Java's
   * serialization and hence is able to encode the {@link Schema} object directly.
   */
  private static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {
    private final Schema schema;

    private SerializableSchemaSupplier(Schema schema) {
      this.schema = schema;
    }

    private Object writeReplace() {
      return new SerializableSchemaString(schema.toString());
    }

    @Override
    public Schema get() {
      return schema;
    }
  }

  // Cache the old encoder/decoder and let the factories reuse them when possible. To be threadsafe,
  // these are ThreadLocal. This code does not need to be re-entrant as AvroCoder does not use
  // an inner coder.
  private final EmptyOnDeserializationThreadLocal<BinaryDecoder> decoder;
  private final EmptyOnDeserializationThreadLocal<BinaryEncoder> encoder;
  private final EmptyOnDeserializationThreadLocal<DatumWriter<T>> writer;
  private final EmptyOnDeserializationThreadLocal<DatumReader<T>> reader;

  protected AvroSpecificCoder(Class<T> type, Schema schema) {
    this.type = type;
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
    typeDescriptor = TypeDescriptor.of(type);

    // Decoder and Encoder start off null for each thread. They are allocated and potentially
    // reused inside encode/decode.
    this.decoder = new EmptyOnDeserializationThreadLocal<>();
    this.encoder = new EmptyOnDeserializationThreadLocal<>();

    // Reader and writer are allocated once per thread per Coder
    this.reader =
        new EmptyOnDeserializationThreadLocal<DatumReader<T>>() {
          private final AvroSpecificCoder<T> myCoder = AvroSpecificCoder.this;

          @Override
          public DatumReader<T> initialValue() {
            return new SpecificDatumReader<>(myCoder.getSchema());
          }
        };

    this.writer =
        new EmptyOnDeserializationThreadLocal<DatumWriter<T>>() {
          private final AvroSpecificCoder<T> myCoder = AvroSpecificCoder.this;

          @Override
          public DatumWriter<T> initialValue() {
            return new SpecificDatumWriter<>(myCoder.getSchema());
          }
        };
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    // Get a BinaryEncoder instance from the ThreadLocal cache and attempt to reuse it.
    BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(outStream, encoder.get());
    // Save the potentially-new instance for reuse later.
    encoder.set(encoderInstance);
    writer.get().write(value, encoderInstance);
    // Direct binary encoder does not buffer any data and need not be flushed.
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    // Get a BinaryDecoder instance from the ThreadLocal cache and attempt to reuse it.
    BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, decoder.get());
    // Save the potentially-new instance for later.
    decoder.set(decoderInstance);
    return reader.get().read(null, decoderInstance);
  }

  @Override
  public void verifyDeterministic() {}

  /** Returns the schema used by this coder. */
  public Schema getSchema() {
    return schemaSupplier.get();
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return typeDescriptor;
  }
}
