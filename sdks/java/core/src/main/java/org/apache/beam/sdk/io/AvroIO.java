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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DynamicDestinationHelpers.ConstantFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <p>To read a {@link PCollection} from one or more Avro files, use {@code AvroIO.read()}, using
 * {@link AvroIO.Read#from} to specify the filename or filepattern to read from. See {@link
 * FileSystems} for information on supported file systems and filepatterns.
 *
 * <p>To read specific records, such as Avro-generated classes, use {@link #read(Class)}. To read
 * {@link GenericRecord GenericRecords}, use {@link #readGenericRecords(Schema)} which takes a
 * {@link Schema} object, or {@link #readGenericRecords(String)} which takes an Avro schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.read(AvroAutoGenClass.class).from("/path/to/file.avro"));
 *
 * // A Read from a GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.readGenericRecords(schema)
 *                .from("gs://my_bucket/path/to/records-*.avro"));
 * }
 * </pre>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use {@link AvroIO.Write}, using
 * {@code AvroIO.write().to(String)} to specify the output filename prefix. The default {@link
 * DefaultFilenamePolicy} will use this prefix, in conjunction with a {@link ShardNameTemplate} (set
 * via {@link Write#withShardNameTemplate(String)}) and optional filename suffix (set via {@link
 * Write#withSuffix(String)}, to generate output filenames in a sharded way. You can override this
 * default write filename policy using {@link
 * Write#to(FileBasedSink.FilenamePolicy)} to specify a custom file naming policy.
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner - {@link AvroIO.Write#withWindowedWrites()}
 * will cause windowing and triggering to be preserved. When producing windowed writes with a
 * streaming runner that supports triggers, the number of output shards must be set explicitly using
 * {@link AvroIO.Write#withNumShards(int)}; some runners may set this for you to a runner-chosen
 * value, so you may need not set it yourself. A {@link FileBasedSink.FilenamePolicy} must be set,
 * and unique windows and triggers must produce unique filenames.
 *
 * <p>To write specific records, such as Avro-generated classes, use {@link #write(Class)}. To write
 * {@link GenericRecord GenericRecords}, use either {@link #writeGenericRecords(Schema)} which takes
 * a {@link Schema} object, or {@link #writeGenericRecords(String)} which takes a schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<AvroAutoGenClass> records = ...;
 * records.apply(AvroIO.write(AvroAutoGenClass.class).to("/path/to/file.avro"));
 *
 * // A Write to a sharded GCS file (runs locally and using remote execution):
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records = ...;
 * records.apply("WriteToAvro", AvroIO.writeGenericRecords(schema)
 *     .to("gs://my_bucket/path/to/numbers")
 *     .withSuffix(".avro"));
 * }
 * </pre>
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the {@link
 * org.apache.avro.file.Codec CodecFactory.deflateCodec(6)}. This default can be changed or
 * overridden using {@link AvroIO.Write#withCodec}.
 */
public class AvroIO {
  private static class IdentityFormatter<T> implements SerializableFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  /**
   * Reads records of the given type from an Avro file (or multiple Avro files matching a pattern).
   *
   * <p>The schema must be specified using one of the {@code withSchema} functions.
   */
  public static <T> Read<T> read(Class<T> recordClass) {
    return new AutoValue_AvroIO_Read.Builder<T>()
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .build();
  }

  /** Reads Avro file(s) containing records of the specified schema. */
  public static Read<GenericRecord> readGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_Read.Builder<GenericRecord>()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .build();
  }

  /**
   * Reads Avro file(s) containing records of the specified schema. The schema is specified as a
   * JSON-encoded string.
   */
  public static Read<GenericRecord> readGenericRecords(String schema) {
    return readGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Writes a {@link PCollection} to an Avro file (or multiple Avro files matching a sharding
   * pattern).
   */
  public static <T> Write<T> write(Class<T> recordClass) {
    return AvroIO.<T>defaultWriteBuilder()
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .build();
  }

  /** Writes Avro records of the specified schema. */
  public static Write<GenericRecord> writeGenericRecords(Schema schema) {
    return AvroIO.<GenericRecord>defaultWriteBuilder()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .build();
  }

  /**
   * Writes Avro records of the specified schema. The schema is specified as a JSON-encoded string.
   */
  public static Write<GenericRecord> writeGenericRecords(String schema) {
    return writeGenericRecords(new Schema.Parser().parse(schema));
  }

  private static <T> Write.Builder<T> defaultWriteBuilder() {
    return new AutoValue_AvroIO_Write.Builder<T>()
        .setFilenameSuffix(null)
        .setShardTemplate(null)
        .setNumShards(0)
        .setCodec(Write.DEFAULT_CODEC)
        .setMetadata(ImmutableMap.<String, Object>of())
        .setWindowedWrites(false);
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract String getFilepattern();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(String filepattern);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);

      abstract Read<T> build();
    }

    /** Reads from the given filename or filepattern. */
    public Read<T> from(String filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException(
            "need to set the filepattern of an AvroIO.Read transform");
      }
      if (getSchema() == null) {
        throw new IllegalStateException("need to set the schema of an AvroIO.Read transform");
      }

      @SuppressWarnings("unchecked")
      Bounded<T> read =
          getRecordClass() == GenericRecord.class
              ? (Bounded<T>) org.apache.beam.sdk.io.Read.from(
                  AvroSource.from(getFilepattern()).withSchema(getSchema()))
              : org.apache.beam.sdk.io.Read.from(
                  AvroSource.from(getFilepattern()).withSchema(getRecordClass()));

      PCollection<T> pcol = input.getPipeline().apply("Read", read);
      // Honor the default output coder that would have been used by this PTransform.
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
        .addIfNotNull(DisplayData.item("filePattern", getFilepattern())
          .withLabel("Input File Pattern"));
    }

    @Override
    protected Coder<T> getDefaultOutputCoder() {
      return AvroCoder.of(getRecordClass(), getSchema());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    private static final SerializableAvroCodecFactory DEFAULT_CODEC =
        new SerializableAvroCodecFactory(CodecFactory.deflateCodec(6));
    // This should be a multiple of 4 to not get a partial encoded byte.
    private static final int METADATA_BYTES_MAX_LENGTH = 40;

    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();
    @Nullable abstract String getShardTemplate();
    @Nullable abstract String getFilenameSuffix();
    @Nullable abstract ValueProvider<ResourceId> getTempDirectory();
    abstract int getNumShards();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();
    abstract boolean getWindowedWrites();
    @Nullable abstract FilenamePolicy getFilenamePolicy();

    /**
     * The codec used to encode the blocks in the Avro file. String value drawn from those in
     * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
     */
    abstract SerializableAvroCodecFactory getCodec();
    /** Avro file metadata. */
    abstract ImmutableMap<String, Object> getMetadata();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);
      abstract Builder<T> setFilenameSuffix(String filenameSuffix);
      abstract Builder<T> setTempDirectory(ValueProvider<ResourceId> tempDirectory);
      abstract Builder<T> setNumShards(int numShards);
      abstract Builder<T> setShardTemplate(String shardTemplate);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);
      abstract Builder<T> setWindowedWrites(boolean windowedWrites);
      abstract Builder<T> setFilenamePolicy(FilenamePolicy filenamePolicy);
      abstract Builder<T> setCodec(SerializableAvroCodecFactory codec);
      abstract Builder<T> setMetadata(ImmutableMap<String, Object> metadata);

      abstract Write<T> build();
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and
     * a common suffix (if supplied using {@link #withSuffix(String)}). This default can be
     * overridden using {@link #to(FilenamePolicy)}.
     */
    public Write<T> to(String outputPrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems. This prefix is used by the {@link DefaultFilenamePolicy}
     * to generate filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and
     * a common suffix (if supplied using {@link #withSuffix(String)}). This default can be
     * overridden using {@link #to(FilenamePolicy)}.
     *
     * <p>This default policy can be overridden using {@link #to(FilenamePolicy)},
     * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
     * not be set. Custom filename policies do not automatically see this prefix - you should
     * explicitly pass the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> to(ResourceId outputPrefix) {
      return toResource(StaticValueProvider.of(outputPrefix));
    }

    /**
     * Like {@link #to(String)}.
     */
    public Write<T> to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /**
     * Like {@link #to(ResourceId)}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> toResource(ValueProvider<ResourceId> outputPrefix) {
      return toBuilder().setFilenamePrefix(outputPrefix).build();
    }

    /**
     * Configures the {@link FileBasedSink.FilenamePolicy} that will be used to name written files.
     */
    public Write<T> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Set the base directory used to generate temporary files.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }


    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<T> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be
     * used when using one of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write<T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /**
     * Configures the number of output shards produced overall (when using unwindowed writes) or
     * per-window (when using windowed writes).
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system decide.
     */
    public Write<T> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template. This option is only compatible
     * with unwindowed writes.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write<T> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public Write<T> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    /** Writes to Avro file(s) compressed using specified codec. */
    public Write<T> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(new SerializableAvroCodecFactory(codec)).build();
    }

    /**
     * Writes to Avro file(s) with the specified metadata.
     *
     * <p>Supported value types are String, Long, and byte[].
     */
    public Write<T> withMetadata(Map<String, Object> metadata) {
      Map<String, String> badKeys = Maps.newLinkedHashMap();
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        Object v = entry.getValue();
        if (!(v instanceof String || v instanceof Long || v instanceof byte[])) {
          badKeys.put(entry.getKey(), v.getClass().getSimpleName());
        }
      }
      checkArgument(
          badKeys.isEmpty(),
          "Metadata value type must be one of String, Long, or byte[]. Found {}",
          badKeys);
      return toBuilder().setMetadata(ImmutableMap.copyOf(metadata)).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a AvroIO.Write "
              + "transform.");
      if (getFilenamePolicy() != null) {
        checkArgument(getShardTemplate() == null && getFilenameSuffix() == null,
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }

      FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
      if (usedFilenamePolicy == null) {
        usedFilenamePolicy =
            DefaultFilenamePolicy.fromStandardParameters(
                getFilenamePrefix(), getShardTemplate(), getFilenameSuffix(), getWindowedWrites());
      }
      DynamicDestinations<T, Void> dynamicDestinations =
          new ConstantFilenamePolicy<>(usedFilenamePolicy);
      return expandTyped(input, dynamicDestinations);
    }

    public <DestinationT> PDone expandTyped(
        PCollection<T> input,
        DynamicDestinations<T, DestinationT> dynamicDestinations) {
      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<T, DestinationT, T> write = WriteFiles.to(
          new AvroSink<>(
              tempDirectory,
              dynamicDestinations,
              AvroCoder.of(getRecordClass(), getSchema()),
              getCodec(),
              getMetadata()), SerializableFunctions.<T>identity());
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (getWindowedWrites()) {
        write = write.withWindowedWrites();
      }
      return input.apply("Write", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      String outputPrefixString = null;
      if (getFilenamePrefix() != null) {
        outputPrefixString = getFilenamePrefix().isAccessible()
        ? getFilenamePrefix().get().toString() : getFilenamePrefix().toString();
      }
      String tempDirectory = null;
      if (getTempDirectory() != null) {
        tempDirectory = getTempDirectory().isAccessible()
            ? getTempDirectory().get().toString() : getTempDirectory().toString();
      }
      builder
          .add(DisplayData.item("schema", getRecordClass())
            .withLabel("Record Schema"))
          .addIfNotNull(DisplayData.item("filePrefix", outputPrefixString)
            .withLabel("Output File Prefix"))
          .addIfNotNull(DisplayData.item("shardNameTemplate", getShardTemplate())
              .withLabel("Output Shard Name Template"))
          .addIfNotNull(DisplayData.item("fileSuffix", getFilenameSuffix())
              .withLabel("Output File Suffix"))
          .addIfNotDefault(DisplayData.item("numShards", getNumShards())
              .withLabel("Maximum Output Shards"),
              0)
          .addIfNotDefault(DisplayData.item("codec", getCodec().toString())
              .withLabel("Avro Compression Codec"),
              DEFAULT_CODEC.toString())
          .addIfNotNull(DisplayData.item("tempDirectory", tempDirectory)
              .withLabel("Directory for temporary files"));
      builder.include("Metadata", new Metadata());
    }

    private class Metadata implements HasDisplayData {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        for (Map.Entry<String, Object> entry : getMetadata().entrySet()) {
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

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private AvroIO() {}
}
