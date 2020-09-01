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

import java.io.Serializable;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Always returns a constant {@link FilenamePolicy}, {@link Schema}, metadata, and codec. */
class ConstantAvroDestination<UserT, OutputT>
    extends DynamicAvroDestinations<UserT, Void, OutputT> {
  private static class SchemaFunction implements Serializable, Function<String, Schema> {
    @Override
    public Schema apply(String input) {
      return new Schema.Parser().parse(input);
    }
  }

  // This should be a multiple of 4 to not get a partial encoded byte.
  private static final int METADATA_BYTES_MAX_LENGTH = 40;
  private final FilenamePolicy filenamePolicy;
  private final Supplier<Schema> schema;
  private final Map<String, Object> metadata;
  private final SerializableAvroCodecFactory codec;
  private final SerializableFunction<UserT, OutputT> formatFunction;
  private final AvroSink.DatumWriterFactory<OutputT> datumWriterFactory;

  private class Metadata implements HasDisplayData {
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        DisplayData.Type type = DisplayData.inferType(entry.getValue());
        if (type != null) {
          builder.add(DisplayData.item(entry.getKey(), type, entry.getValue()));
        } else {
          String base64 = BaseEncoding.base64().encode((byte[]) entry.getValue());
          String repr =
              base64.length() <= METADATA_BYTES_MAX_LENGTH
                  ? base64
                  : base64.substring(0, METADATA_BYTES_MAX_LENGTH) + "...";
          builder.add(DisplayData.item(entry.getKey(), repr));
        }
      }
    }
  }

  public ConstantAvroDestination(
      FilenamePolicy filenamePolicy,
      Schema schema,
      Map<String, Object> metadata,
      CodecFactory codec,
      SerializableFunction<UserT, OutputT> formatFunction) {
    this(filenamePolicy, schema, metadata, codec, formatFunction, null);
  }

  public ConstantAvroDestination(
      FilenamePolicy filenamePolicy,
      Schema schema,
      Map<String, Object> metadata,
      CodecFactory codec,
      SerializableFunction<UserT, OutputT> formatFunction,
      AvroSink.@Nullable DatumWriterFactory<OutputT> datumWriterFactory) {
    this.filenamePolicy = filenamePolicy;
    this.schema = Suppliers.compose(new SchemaFunction(), Suppliers.ofInstance(schema.toString()));
    this.metadata = metadata;
    this.codec = new SerializableAvroCodecFactory(codec);
    this.formatFunction = formatFunction;
    this.datumWriterFactory = datumWriterFactory;
  }

  @Override
  public OutputT formatRecord(UserT record) {
    return formatFunction.apply(record);
  }

  @Override
  public @Nullable Void getDestination(UserT element) {
    return (Void) null;
  }

  @Override
  public @Nullable Void getDefaultDestination() {
    return (Void) null;
  }

  @Override
  public FilenamePolicy getFilenamePolicy(Void destination) {
    return filenamePolicy;
  }

  @Override
  public Schema getSchema(Void destination) {
    return schema.get();
  }

  @Override
  public Map<String, Object> getMetadata(Void destination) {
    return metadata;
  }

  @Override
  public CodecFactory getCodec(Void destination) {
    return codec.getCodec();
  }

  @Override
  public AvroSink.@Nullable DatumWriterFactory<OutputT> getDatumWriterFactory(Void destination) {
    return datumWriterFactory;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    filenamePolicy.populateDisplayData(builder);
    builder.add(DisplayData.item("schema", schema.get().toString()).withLabel("Record Schema"));
    builder.addIfNotDefault(
        DisplayData.item("codec", codec.getCodec().toString()).withLabel("Avro Compression Codec"),
        AvroIO.TypedWrite.DEFAULT_SERIALIZABLE_CODEC.toString());
    builder.include("Metadata", new Metadata());
  }
}
