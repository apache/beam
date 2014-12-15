/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.util.CloudObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * An encoder using Avro binary format.
 * <p>
 * The Avro schema is generated using reflection on the element type, using
 * Avro's <a href="http://avro.apache.org/docs/current/api/java/index.html">
 * org.apache.avro.reflect.ReflectData</a>,
 * and encoded as part of the {@code Coder} instance.
 * <p>
 * For complete details about schema generation and how it can be controlled please see
 * the <a href="http://avro.apache.org/docs/current/api/java/index.html">
 * org.apache.avro.reflect package</a>.
 * Only concrete classes with a no-argument constructor can be mapped to Avro records.
 * All inherited fields that are not static or transient are used. Fields are not permitted to be
 * null unless annotated by
 * <a href="http://avro.apache.org/docs/current/api/java/org/apache/avro/reflect/Nullable.html">
 * org.apache.avro.reflect.Nullable</a> or a
 * <a href="http://avro.apache.org/docs/current/api/java/org/apache/avro/reflect/Union.html">
 * org.apache.avro.reflect.Union</a> containing null.
 * <p>
 * To use, specify the {@code Coder} type on a PCollection:
 * <pre>
 * {@code
 * PCollection<MyCustomElement> records =
 *     input.apply(...)
 *          .setCoder(AvroCoder.of(MyCustomElement.class);
 * }
 * </pre>
 * <p>
 * or annotate the element class using {@code @DefaultCoder}.
 * <pre><code>
 * {@literal @}DefaultCoder(AvroCoder.class)
 * public class MyCustomElement {
 *   ...
 * }
 * </code></pre>
 *
 * @param <T> the type of elements handled by this coder
 */
@SuppressWarnings("serial")
public class AvroCoder<T> extends StandardCoder<T> {
  /**
   * Returns an {@code AvroCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T> AvroCoder<T> of(Class<T> type) {
    return new AvroCoder<>(type, ReflectData.get().getSchema(type));
  }

  /**
   * Returns an {@code AvroCoder} instance for the Avro schema. The implicit
   * type is GenericRecord.
   */
  public static AvroCoder<GenericRecord> of(Schema schema) {
    return new AvroCoder<>(GenericRecord.class, schema);
  }

  /**
   * Returns an {@code AvroCoder} instance for the provided element type
   * using the provided Avro schema.
   *
   * <p> If the type argument is GenericRecord, the schema may be arbitrary.
   * Otherwise, the schema must correspond to the type provided.
   *
   * @param <T> the element type
   */
  public static <T> AvroCoder<T> of(Class<T> type, Schema schema) {
    return new AvroCoder<>(type, schema);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @JsonCreator
  public static AvroCoder<?> of(
      @JsonProperty("type") String classType,
      @JsonProperty("schema") String schema) throws ClassNotFoundException {
    Schema.Parser parser = new Schema.Parser();
    return new AvroCoder(Class.forName(classType), parser.parse(schema));
  }

  private final Class<T> type;
  private final Schema schema;
  private final DatumWriter<T> writer;
  private final DatumReader<T> reader;
  private final EncoderFactory encoderFactory = new EncoderFactory();
  private final DecoderFactory decoderFactory = new DecoderFactory();

  protected AvroCoder(Class<T> type, Schema schema) {
    this.type = type;
    this.schema = schema;
    this.reader = createDatumReader();
    this.writer = createDatumWriter();
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws IOException {
    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outStream, null);
    writer.write(value, encoder);
    encoder.flush();
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    BinaryDecoder decoder = decoderFactory.directBinaryDecoder(inStream, null);
    return reader.read(null, decoder);
  }

  @Override
    public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addString(result, "type", type.getName());
    addString(result, "schema", schema.toString());
    return result;
  }

  /**
   * Depends upon the structure being serialized.
   */
  @Override
  public boolean isDeterministic() {
    return false;
  }

  /**
   * Returns a new DatumReader that can be used to read from
   * an Avro file directly.
   */
  public DatumReader<T> createDatumReader() {
    if (type.equals(GenericRecord.class)) {
      return new GenericDatumReader<>(schema);
    } else {
      return new ReflectDatumReader<>(schema);
    }
  }

  /**
   * Returns a new DatumWriter that can be used to write to
   * an Avro file directly.
   */
  public DatumWriter<T> createDatumWriter() {
    if (type.equals(GenericRecord.class)) {
      return new GenericDatumWriter<>(schema);
    } else {
      return new ReflectDatumWriter<>(schema);
    }
  }

  /**
   * Returns the schema used by this coder.
   */
  public Schema getSchema() {
    return schema;
  }
}
