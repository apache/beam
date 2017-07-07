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

package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Supplier;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.ParamsCoder;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/** Some helper classes that derive from {@link FileBasedSink.DynamicDestinations}. */
public class DynamicFileDestinations {
  /** Always returns a constant {@link FilenamePolicy}. */
  private static class ConstantFilenamePolicy<T> extends DynamicDestinations<T, Void> {
    private final FilenamePolicy filenamePolicy;

    public ConstantFilenamePolicy(FilenamePolicy filenamePolicy) {
      this.filenamePolicy = checkNotNull(filenamePolicy);
    }

    @Override
    public Void getDestination(T element) {
      return (Void) null;
    }

    @Override
    public Coder<Void> getDestinationCoder() {
      return null;
    }

    @Override
    public Void getDefaultDestination() {
      return (Void) null;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Void destination) {
      return filenamePolicy;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      checkState(filenamePolicy != null);
      filenamePolicy.populateDisplayData(builder);
    }
  }

  // TODO: These two classes are duplicated in AvroCoder. Is there a common place to put them?
  private static class SerializableSchemaString implements Serializable {
    private final String schema;
    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(Schema.parse(schema));
    }
  }

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

  /**
   * Always returns a constant {@link FilenamePolicy}, {@link Schema}, metadata, and codec.
   */
  private static class ConstantAvroDestination<T> extends DynamicAvroDestinations<T, Void> {
    // This should be a multiple of 4 to not get a partial encoded byte.
    private static final int METADATA_BYTES_MAX_LENGTH = 40;
    private final FilenamePolicy filenamePolicy;
    private final SerializableSchemaSupplier schema;
    private final Map<String, Object> metadata;
    private final SerializableAvroCodecFactory codec;

    private class Metadata implements HasDisplayData {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
          DisplayData.Type type = DisplayData.inferType(entry.getValue());
          if (type != null) {
            builder.add(DisplayData.item(entry.getKey(), type, entry.getValue()));
          } else {
            String base64 = BaseEncoding.base64().encode((byte[]) entry.getValue());
            String repr = base64.length() <= METADATA_BYTES_MAX_LENGTH
                ? base64 : base64.substring(0, METADATA_BYTES_MAX_LENGTH) + "...";
            builder.add(DisplayData.item(entry.getKey(), repr));
          }
        }
      }
    }

    public ConstantAvroDestination(FilenamePolicy filenamePolicy, Schema schema,
                                   Map<String, Object> metadata, CodecFactory codec) {
      this.filenamePolicy = filenamePolicy;
      this.schema = new SerializableSchemaSupplier(schema);
      this.metadata = metadata;
      this.codec = new SerializableAvroCodecFactory(codec);
    }

    @Override
    public Void getDestination(T element) {
      return (Void) null;
    }

    @Override
    public Void getDefaultDestination() {
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
    public void populateDisplayData(DisplayData.Builder builder) {
      filenamePolicy.populateDisplayData(builder);
      builder.add(DisplayData.item("schema", schema.get().toString()).withLabel("Record Schema"));
      builder.addIfNotDefault(
          DisplayData.item("codec", codec.getCodec().toString())
              .withLabel("Avro Compression Codec"),
          AvroIO.TypedWrite.DEFAULT_SERIALIZABLE_CODEC.toString());
      builder.include("Metadata", new Metadata());
    }
  }

  /**
   * A base class for a {@link DynamicDestinations} object that returns differently-configured
   * instances of {@link DefaultFilenamePolicy}.
   */
  private static class DefaultPolicyDestinations<UserT> extends DynamicDestinations<UserT, Params> {
    SerializableFunction<UserT, Params> destinationFunction;
    Params emptyDestination;

    public DefaultPolicyDestinations(
        SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
      this.destinationFunction = destinationFunction;
      this.emptyDestination = emptyDestination;
    }

    @Override
    public Params getDestination(UserT element) {
      return destinationFunction.apply(element);
    }

    @Override
    public Params getDefaultDestination() {
      return emptyDestination;
    }

    @Nullable
    @Override
    public Coder<Params> getDestinationCoder() {
      return ParamsCoder.of();
    }

    @Override
    public FilenamePolicy getFilenamePolicy(DefaultFilenamePolicy.Params params) {
      return DefaultFilenamePolicy.fromParams(params);
    }
  }

  /**
   * Returns a {@link DynamicDestinations} that always returns the same {@link FilenamePolicy}.
   */
  public static <T> DynamicDestinations<T, Void> constant(FilenamePolicy filenamePolicy) {
    return new ConstantFilenamePolicy<>(filenamePolicy);
  }


  /**
   * Returns a {@link DynamicAvroDestinations} that always returns the same
   * {@link FilenamePolicy}, schema, metadata, and codec.
   */
  public static <T> DynamicAvroDestinations<T, Void> constantAvros(
      FilenamePolicy filenamePolicy, Schema schema, Map<String, Object> metadata,
      CodecFactory codec) {
    return new ConstantAvroDestination<>(filenamePolicy, schema, metadata, codec);
  }

  /**
   * Returns a {@link DynamicDestinations} that returns instances of {@link DefaultFilenamePolicy}
   * configured with the given {@link Params}.
   */
  public static <UserT> DynamicDestinations<UserT, Params> toDefaultPolicies(
      SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
    return new DefaultPolicyDestinations<>(destinationFunction, emptyDestination);
  }
}
