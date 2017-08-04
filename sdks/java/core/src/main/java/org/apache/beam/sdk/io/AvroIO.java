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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <p>To read a {@link PCollection} from one or more Avro files with the same schema known at
 * pipeline construction time, use {@code AvroIO.read()}, using {@link AvroIO.Read#from} to specify
 * the filename or filepattern to read from. Alternatively, if the filepatterns to be read are
 * themselves in a {@link PCollection}, apply {@link #readAll}.
 *
 * <p>See {@link FileSystems} for information on supported file systems and filepatterns.
 *
 * <p>To read specific records, such as Avro-generated classes, use {@link #read(Class)}. To read
 * {@link GenericRecord GenericRecords}, use {@link #readGenericRecords(Schema)} which takes a
 * {@link Schema} object, or {@link #readGenericRecords(String)} which takes an Avro schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema. Likewise, to read a {@link PCollection} of filepatterns, apply {@link
 * #readAllGenericRecords}.
 *
 * <p>To read records from files whose schema is unknown at pipeline construction time or differs
 * between files, use {@link #parseGenericRecords} - in this case, you will need to specify a
 * parsing function for converting each {@link GenericRecord} into a value of your custom type.
 * Likewise, to read a {@link PCollection} of filepatterns with unknown schema, use {@link
 * #parseAllGenericRecords}.
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
 *
 * PCollection<Foo> records =
 *     p.apply(AvroIO.parseGenericRecords(new SerializableFunction<GenericRecord, Foo>() {
 *       public Foo apply(GenericRecord record) {
 *         // If needed, access the schema of the record using record.getSchema()
 *         return ...;
 *       }
 *     }));
 * }</pre>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link Read#withHintMatchesManyFiles} or {@link
 * Parse#withHintMatchesManyFiles} for better performance and scalability. Note that it may decrease
 * performance if the filepattern matches only a small number of files.
 *
 * <p>Reading from a {@link PCollection} of filepatterns:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> filepatterns = p.apply(...);
 * PCollection<AvroAutoGenClass> records =
 *     filepatterns.apply(AvroIO.read(AvroAutoGenClass.class));
 * PCollection<GenericRecord> genericRecords =
 *     filepatterns.apply(AvroIO.readGenericRecords(schema));
 * PCollection<Foo> records =
 *     filepatterns.apply(AvroIO.parseAllGenericRecords(new SerializableFunction...);
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use {@link AvroIO.Write}, using
 * {@code AvroIO.write().to(String)} to specify the output filename prefix. The default {@link
 * DefaultFilenamePolicy} will use this prefix, in conjunction with a {@link ShardNameTemplate} (set
 * via {@link Write#withShardNameTemplate(String)}) and optional filename suffix (set via {@link
 * Write#withSuffix(String)}, to generate output filenames in a sharded way. You can override this
 * default write filename policy using {@link Write#to(FileBasedSink.FilenamePolicy)} to specify a
 * custom file naming policy.
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
 * }</pre>
 *
 * <p>The following shows a more-complex example of AvroIO.Write usage, generating dynamic file
 * destinations as well as a dynamic Avro schema per file. In this example, a PCollection of user
 * events (e.g. actions on a website) is written out to Avro files. Each event contains the user id
 * as an integer field. We want events for each user to go into a specific directory for that user,
 * and each user's data should be written with a specific schema for that user; a side input is
 * used, so the schema can be calculated in a different stage.
 *
 * <pre>{@code
 * // This is the user class that controls dynamic destinations for this avro write. The input to
 * // AvroIO.Write will be UserEvent, and we will be writing GenericRecords to the file (in order
 * // to have dynamic schemas). Everything is per userid, so we define a dynamic destination type
 * // of Integer.
 * class UserDynamicAvroDestinations
 *     extends DynamicAvroDestinations<UserEvent, Integer, GenericRecord> {
 *   private final PCollectionView<Map<Integer, String>> userToSchemaMap;
 *   public UserDynamicAvroDestinations( PCollectionView<Map<Integer, String>> userToSchemaMap) {
 *     this.userToSchemaMap = userToSchemaMap;
 *   }
 *   public GenericRecord formatRecord(UserEvent record) {
 *     return formatUserRecord(record, getSchema(record.getUserId()));
 *   }
 *   public Schema getSchema(Integer userId) {
 *     return new Schema.Parser().parse(sideInput(userToSchemaMap).get(userId));
 *   }
 *   public Integer getDestination(UserEvent record) {
 *     return record.getUserId();
 *   }
 *   public Integer getDefaultDestination() {
 *     return 0;
 *   }
 *   public FilenamePolicy getFilenamePolicy(Integer userId) {
 *     return DefaultFilenamePolicy.fromParams(new Params().withBaseFilename(baseDir + "/user-"
 *     + userId + "/events"));
 *   }
 *   public List<PCollectionView<?>> getSideInputs() {
 *     return ImmutableList.<PCollectionView<?>>of(userToSchemaMap);
 *   }
 * }
 * PCollection<UserEvents> events = ...;
 * PCollectionView<Integer, String> schemaMap = events.apply(
 *     "ComputeSchemas", new ComputePerUserSchemas());
 * events.apply("WriteAvros", AvroIO.<Integer>writeCustomTypeToGenericRecords()
 *     .to(new UserDynamicAvros()));
 * }</pre>
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the {@link
 * org.apache.avro.file.Codec CodecFactory.deflateCodec(6)}. This default can be changed or
 * overridden using {@link AvroIO.Write#withCodec}.
 */
public class AvroIO {
  /**
   * Reads records of the given type from an Avro file (or multiple Avro files matching a pattern).
   *
   * <p>The schema must be specified using one of the {@code withSchema} functions.
   */
  public static <T> Read<T> read(Class<T> recordClass) {
    return new AutoValue_AvroIO_Read.Builder<T>()
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .setHintMatchesManyFiles(false)
        .build();
  }

  /** Like {@link #read}, but reads each filepattern in the input {@link PCollection}. */
  public static <T> ReadAll<T> readAll(Class<T> recordClass) {
    return new AutoValue_AvroIO_ReadAll.Builder<T>()
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /** Reads Avro file(s) containing records of the specified schema. */
  public static Read<GenericRecord> readGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_Read.Builder<GenericRecord>()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #readGenericRecords(Schema)}, but reads each filepattern in the input {@link
   * PCollection}.
   */
  public static ReadAll<GenericRecord> readAllGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_ReadAll.Builder<GenericRecord>()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
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
   * Like {@link #readGenericRecords(String)}, but reads each filepattern in the input {@link
   * PCollection}.
   */
  public static ReadAll<GenericRecord> readAllGenericRecords(String schema) {
    return readAllGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Reads Avro file(s) containing records of an unspecified schema and converting each record to a
   * custom type.
   */
  public static <T> Parse<T> parseGenericRecords(SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_Parse.Builder<T>()
        .setParseFn(parseFn)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each filepattern in the
   * input {@link PCollection}.
   */
  public static <T> ParseAll<T> parseAllGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_ParseAll.Builder<T>()
        .setParseFn(parseFn)
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * Writes a {@link PCollection} to an Avro file (or multiple Avro files matching a sharding
   * pattern).
   */
  public static <T> Write<T> write(Class<T> recordClass) {
    return new Write<>(
        AvroIO.<T, T>defaultWriteBuilder()
            .setGenericRecords(false)
            .setSchema(ReflectData.get().getSchema(recordClass))
            .build());
  }

  /** Writes Avro records of the specified schema. */
  public static Write<GenericRecord> writeGenericRecords(Schema schema) {
    return new Write<>(
        AvroIO.<GenericRecord, GenericRecord>defaultWriteBuilder()
            .setGenericRecords(true)
            .setSchema(schema)
            .build());
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to an avro file (or multiple avro files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * record of type OutputT.
   *
   * <p>This version allows you to apply {@link AvroIO} writes to a PCollection of a custom type
   * {@link UserT}. A format mechanism that converts the input type {@link UserT} to the output type
   * that will be written to the file must be specified. If using a custom {@link
   * DynamicAvroDestinations} object this is done using {@link
   * DynamicAvroDestinations#formatRecord}, otherwise the {@link
   * AvroIO.TypedWrite#withFormatFunction} can be used to specify a format function.
   *
   * <p>The advantage of using a custom type is that is it allows a user-provided {@link
   * DynamicAvroDestinations} object, set via {@link AvroIO.Write#to(DynamicAvroDestinations)} to
   * examine the custom type when choosing a destination.
   *
   * <p>If the output type is {@link GenericRecord} use {@link #writeCustomTypeToGenericRecords()}
   * instead.
   */
  public static <UserT, OutputT> TypedWrite<UserT, OutputT> writeCustomType() {
    return AvroIO.<UserT, OutputT>defaultWriteBuilder().setGenericRecords(false).build();
  }

  /**
   * Similar to {@link #writeCustomType()}, but specialized for the case where the output type is
   * {@link GenericRecord}. A schema must be specified either in {@link
   * DynamicAvroDestinations#getSchema} or if not using dynamic destinations, by using {@link
   * TypedWrite#withSchema(Schema)}.
   */
  public static <UserT> TypedWrite<UserT, GenericRecord> writeCustomTypeToGenericRecords() {
    return AvroIO.<UserT, GenericRecord>defaultWriteBuilder().setGenericRecords(true).build();
  }

  /**
   * Writes Avro records of the specified schema. The schema is specified as a JSON-encoded string.
   */
  public static Write<GenericRecord> writeGenericRecords(String schema) {
    return writeGenericRecords(new Schema.Parser().parse(schema));
  }

  private static <UserT, OutputT> TypedWrite.Builder<UserT, OutputT> defaultWriteBuilder() {
    return new AutoValue_AvroIO_TypedWrite.Builder<UserT, OutputT>()
        .setFilenameSuffix(null)
        .setShardTemplate(null)
        .setNumShards(0)
        .setCodec(TypedWrite.DEFAULT_SERIALIZABLE_CODEC)
        .setMetadata(ImmutableMap.<String, Object>of())
        .setWindowedWrites(false);
  }

  /** Implementation of {@link #read} and {@link #readGenericRecords}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();
    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);
      abstract Builder<T> setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Read<T> build();
    }

    /**
     * Reads from the given filename or filepattern.
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
     */
    public Read<T> from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Like {@link #from(ValueProvider)}. */
    public Read<T> from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }

    /**
     * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
     * files.
     *
     * <p>This hint may cause a runner to execute the transform differently, in a way that improves
     * performance for this case, but it may worsen performance if the filepattern matches only a
     * small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
     * happen less efficiently within individual files).
     */
    public Read<T> withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "filepattern");
      checkNotNull(getSchema(), "schema");
      if (getHintMatchesManyFiles()) {
        ReadAll<T> readAll =
            (getRecordClass() == GenericRecord.class)
                ? (ReadAll<T>) readAllGenericRecords(getSchema())
                : readAll(getRecordClass());
        return input
            .apply(Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
            .apply(readAll);
      } else {
        return input
            .getPipeline()
            .apply(
                "Read",
                org.apache.beam.sdk.io.Read.from(
                    createSource(getFilepattern(), getRecordClass(), getSchema())));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"));
    }

    @SuppressWarnings("unchecked")
    private static <T> AvroSource<T> createSource(
        ValueProvider<String> filepattern, Class<T> recordClass, Schema schema) {
      return recordClass == GenericRecord.class
          ? (AvroSource<T>) AvroSource.from(filepattern).withSchema(schema)
          : AvroSource.from(filepattern).withSchema(recordClass);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    @Nullable abstract Class<T> getRecordClass();
    @Nullable abstract Schema getSchema();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setRecordClass(Class<T> recordClass);
      abstract Builder<T> setSchema(Schema schema);
      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ReadAll<T> build();
    }

    @VisibleForTesting
    ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      checkNotNull(getSchema(), "schema");
      return input
          .apply(
              "Read all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  SerializableFunctions.<String, Boolean>constant(true) /* isSplittable */,
                  getDesiredBundleSizeBytes(),
                  new CreateSourceFn<>(getRecordClass(), getSchema().toString())))
          .setCoder(AvroCoder.of(getRecordClass(), getSchema()));
    }
  }

  private static class CreateSourceFn<T>
      implements SerializableFunction<String, FileBasedSource<T>> {
    private final Class<T> recordClass;
    private final Supplier<Schema> schemaSupplier;

    public CreateSourceFn(Class<T> recordClass, String jsonSchema) {
      this.recordClass = recordClass;
      this.schemaSupplier = AvroUtils.serializableSchemaSupplier(jsonSchema);
    }

    @Override
    public FileBasedSource<T> apply(String input) {
      return Read.createSource(
          StaticValueProvider.of(input), recordClass, schemaSupplier.get());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseGenericRecords}. */
  @AutoValue
  public abstract static class Parse<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract SerializableFunction<GenericRecord, T> getParseFn();
    @Nullable abstract Coder<T> getCoder();
    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setHintMatchesManyFiles(boolean hintMatchesManyFiles);

      abstract Parse<T> build();
    }

    /** Reads from the given filename or filepattern. */
    public Parse<T> from(String filepattern) {
      return from(StaticValueProvider.of(filepattern));
    }

    /** Like {@link #from(String)}. */
    public Parse<T> from(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets a coder for the result of the parse function. */
    public Parse<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /** Like {@link Read#withHintMatchesManyFiles()}. */
    public Parse<T> withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "filepattern");
      Coder<T> coder = inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());
      if (getHintMatchesManyFiles()) {
        return input
            .apply(Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
            .apply(parseAllGenericRecords(getParseFn()).withCoder(getCoder()));
      }
      return input.apply(
          org.apache.beam.sdk.io.Read.from(
              AvroSource.from(getFilepattern()).withParseFn(getParseFn(), coder)));
    }

    private static <T> Coder<T> inferCoder(
        @Nullable Coder<T> explicitCoder,
        SerializableFunction<GenericRecord, T> parseFn,
        CoderRegistry coderRegistry) {
      if (explicitCoder != null) {
        return explicitCoder;
      }
      // If a coder was not specified explicitly, infer it from parse fn.
      TypeDescriptor<T> descriptor = TypeDescriptors.outputOf(parseFn);
      String message =
          "Unable to infer coder for output of parseFn. Specify it explicitly using withCoder().";
      checkArgument(descriptor != null, message);
      try {
        return coderRegistry.getCoder(descriptor);
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(message, e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseAllGenericRecords}. */
  @AutoValue
  public abstract static class ParseAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    abstract SerializableFunction<GenericRecord, T> getParseFn();
    @Nullable abstract Coder<T> getCoder();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ParseAll<T> build();
    }

    /** Specifies the coder for the result of the {@code parseFn}. */
    public ParseAll<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @VisibleForTesting
    ParseAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      final Coder<T> coder =
          Parse.inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());
      SerializableFunction<String, FileBasedSource<T>> createSource =
          new SerializableFunction<String, FileBasedSource<T>>() {
            @Override
            public FileBasedSource<T> apply(String input) {
              return AvroSource.from(input).withParseFn(getParseFn(), coder);
            }
          };
      return input
          .apply(
              "Parse all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  SerializableFunctions.<String, Boolean>constant(true) /* isSplittable */,
                  getDesiredBundleSizeBytes(),
                  createSource))
          .setCoder(coder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
    }
  }

  // ///////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class TypedWrite<UserT, OutputT>
      extends PTransform<PCollection<UserT>, PDone> {
    static final CodecFactory DEFAULT_CODEC = CodecFactory.deflateCodec(6);
    static final SerializableAvroCodecFactory DEFAULT_SERIALIZABLE_CODEC =
        new SerializableAvroCodecFactory(DEFAULT_CODEC);

    @Nullable
    abstract SerializableFunction<UserT, OutputT> getFormatFunction();

    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();
    @Nullable abstract String getShardTemplate();
    @Nullable abstract String getFilenameSuffix();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectory();

    abstract int getNumShards();

    abstract boolean getGenericRecords();

    @Nullable abstract Schema getSchema();
    abstract boolean getWindowedWrites();
    @Nullable abstract FilenamePolicy getFilenamePolicy();

    @Nullable
    abstract DynamicAvroDestinations<UserT, ?, OutputT> getDynamicDestinations();

    /**
     * The codec used to encode the blocks in the Avro file. String value drawn from those in
     * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
     */
    abstract SerializableAvroCodecFactory getCodec();
    /** Avro file metadata. */
    abstract ImmutableMap<String, Object> getMetadata();

    abstract Builder<UserT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT, OutputT> {
      abstract Builder<UserT, OutputT> setFormatFunction(
          SerializableFunction<UserT, OutputT> formatFunction);

      abstract Builder<UserT, OutputT> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);

      abstract Builder<UserT, OutputT> setFilenameSuffix(String filenameSuffix);

      abstract Builder<UserT, OutputT> setTempDirectory(ValueProvider<ResourceId> tempDirectory);

      abstract Builder<UserT, OutputT> setNumShards(int numShards);

      abstract Builder<UserT, OutputT> setShardTemplate(String shardTemplate);

      abstract Builder<UserT, OutputT> setGenericRecords(boolean genericRecords);

      abstract Builder<UserT, OutputT> setSchema(Schema schema);

      abstract Builder<UserT, OutputT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT, OutputT> setFilenamePolicy(FilenamePolicy filenamePolicy);

      abstract Builder<UserT, OutputT> setCodec(SerializableAvroCodecFactory codec);

      abstract Builder<UserT, OutputT> setMetadata(ImmutableMap<String, Object> metadata);

      abstract Builder<UserT, OutputT> setDynamicDestinations(
          DynamicAvroDestinations<UserT, ?, OutputT> dynamicDestinations);

      abstract TypedWrite<UserT, OutputT> build();
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
     * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
     * using {@link #to(FilenamePolicy)}.
     */
    public TypedWrite<UserT, OutputT> to(String outputPrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix));
    }

    /**
     * Writes to file(s) with the given output prefix. See {@link FileSystems} for information on
     * supported file systems. This prefix is used by the {@link DefaultFilenamePolicy} to generate
     * filenames.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will build output filenames using the
     * specified prefix, a shard name template (see {@link #withShardNameTemplate(String)}, and a
     * common suffix (if supplied using {@link #withSuffix(String)}). This default can be overridden
     * using {@link #to(FilenamePolicy)}.
     *
     * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
     * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
     * Custom filename policies do not automatically see this prefix - you should explicitly pass
     * the prefix into your {@link FilenamePolicy} object if you need this.
     *
     * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
     * infer a directory for temporary files.
     */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> to(ResourceId outputPrefix) {
      return toResource(StaticValueProvider.of(outputPrefix));
    }

    /** Like {@link #to(String)}. */
    public TypedWrite<UserT, OutputT> to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /** Like {@link #to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> toResource(ValueProvider<ResourceId> outputPrefix) {
      return toBuilder().setFilenamePrefix(outputPrefix).build();
    }

    /**
     * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
     * directory for temporary files must be specified using {@link #withTempDirectory}.
     */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Use a {@link DynamicAvroDestinations} object to vend {@link FilenamePolicy} objects. These
     * objects can examine the input record when creating a {@link FilenamePolicy}. A directory for
     * temporary files must be specified using {@link #withTempDirectory}.
     */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> to(
        DynamicAvroDestinations<UserT, ?, OutputT> dynamicDestinations) {
      return toBuilder().setDynamicDestinations(dynamicDestinations).build();
    }

    /**
     * Sets the the output schema. Can only be used when the output type is {@link GenericRecord}
     * and when not using {@link #to(DynamicAvroDestinations)}.
     */
    public TypedWrite<UserT, OutputT> withSchema(Schema schema) {
      return toBuilder().setSchema(schema).build();
    }

    /**
     * Specifies a format function to convert {@link UserT} to the output type. If {@link
     * #to(DynamicAvroDestinations)} is used, {@link DynamicAvroDestinations#formatRecord} must be
     * used instead.
     */
    public TypedWrite<UserT, OutputT> withFormatFunction(
        SerializableFunction<UserT, OutputT> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, OutputT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT, OutputT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when using one
     * of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT, OutputT> withSuffix(String filenameSuffix) {
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
    public TypedWrite<UserT, OutputT> withNumShards(int numShards) {
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
    public TypedWrite<UserT, OutputT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public TypedWrite<UserT, OutputT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    /** Writes to Avro file(s) compressed using specified codec. */
    public TypedWrite<UserT, OutputT> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(new SerializableAvroCodecFactory(codec)).build();
    }

    /**
     * Writes to Avro file(s) with the specified metadata.
     *
     * <p>Supported value types are String, Long, and byte[].
     */
    public TypedWrite<UserT, OutputT> withMetadata(Map<String, Object> metadata) {
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

    DynamicAvroDestinations<UserT, ?, OutputT> resolveDynamicDestinations() {
      DynamicAvroDestinations<UserT, ?, OutputT> dynamicDestinations = getDynamicDestinations();
      if (dynamicDestinations == null) {
        FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
        if (usedFilenamePolicy == null) {
          usedFilenamePolicy =
              DefaultFilenamePolicy.fromStandardParameters(
                  getFilenamePrefix(),
                  getShardTemplate(),
                  getFilenameSuffix(),
                  getWindowedWrites());
        }
        dynamicDestinations =
            constantDestinations(
                usedFilenamePolicy,
                getSchema(),
                getMetadata(),
                getCodec().getCodec(),
                getFormatFunction());
      }
      return dynamicDestinations;
    }

    @Override
    public PDone expand(PCollection<UserT> input) {
      checkArgument(
          getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a AvroIO.Write "
              + "transform.");
      if (getFilenamePolicy() != null) {
        checkArgument(
            getShardTemplate() == null && getFilenameSuffix() == null,
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }
      if (getDynamicDestinations() != null) {
        checkArgument(
            getFormatFunction() == null,
            "A format function should not be specified "
                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
      }

      return expandTyped(input, resolveDynamicDestinations());
    }

    public <DestinationT> PDone expandTyped(
        PCollection<UserT> input,
        DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations) {
      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<UserT, DestinationT, OutputT> write =
          WriteFiles.to(new AvroSink<>(tempDirectory, dynamicDestinations, getGenericRecords()));
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
      resolveDynamicDestinations().populateDisplayData(builder);

      String tempDirectory = null;
      if (getTempDirectory() != null) {
        tempDirectory =
            getTempDirectory().isAccessible()
                ? getTempDirectory().get().toString()
                : getTempDirectory().toString();
      }
      builder
          .addIfNotDefault(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(
              DisplayData.item("tempDirectory", tempDirectory)
                  .withLabel("Directory for temporary files"));
    }
  }

  /**
   * This class is used as the default return value of {@link AvroIO#write}
   *
   * <p>All methods in this class delegate to the appropriate method of {@link AvroIO.TypedWrite}.
   * This class exists for backwards compatibility, and will be removed in Beam 3.0.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @VisibleForTesting TypedWrite<T, T> inner;

    Write(TypedWrite<T, T> inner) {
      this.inner = inner;
    }

    /** See {@link TypedWrite#to(String)}. */
    public Write<T> to(String outputPrefix) {
      return new Write<>(
          inner
              .to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix))
              .withFormatFunction(SerializableFunctions.<T>identity()));
    }

    /** See {@link TypedWrite#to(ResourceId)} . */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> to(ResourceId outputPrefix) {
      return new Write<T>(
          inner.to(outputPrefix).withFormatFunction(SerializableFunctions.<T>identity()));
    }

    /** See {@link TypedWrite#to(ValueProvider)}. */
    public Write<T> to(ValueProvider<String> outputPrefix) {
      return new Write<>(
          inner.to(outputPrefix).withFormatFunction(SerializableFunctions.<T>identity()));
    }

    /** See {@link TypedWrite#to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> toResource(ValueProvider<ResourceId> outputPrefix) {
      return new Write<>(
          inner.toResource(outputPrefix).withFormatFunction(SerializableFunctions.<T>identity()));
    }

    /** See {@link TypedWrite#to(FilenamePolicy)}. */
    public Write<T> to(FilenamePolicy filenamePolicy) {
      return new Write<>(
          inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.<T>identity()));
    }

    /** See {@link TypedWrite#to(DynamicAvroDestinations)}. */
    public Write to(DynamicAvroDestinations<T, ?, T> dynamicDestinations) {
      return new Write<>(inner.to(dynamicDestinations).withFormatFunction(null));
    }

    /** See {@link TypedWrite#withSchema}. */
    public Write withSchema(Schema schema) {
      return new Write<>(inner.withSchema(schema));
    }
    /** See {@link TypedWrite#withTempDirectory(ValueProvider)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return new Write<>(inner.withTempDirectory(tempDirectory));
    }

    /** See {@link TypedWrite#withTempDirectory(ResourceId)}. */
    public Write<T> withTempDirectory(ResourceId tempDirectory) {
      return new Write<>(inner.withTempDirectory(tempDirectory));
    }

    /** See {@link TypedWrite#withShardNameTemplate}. */
    public Write<T> withShardNameTemplate(String shardTemplate) {
      return new Write<>(inner.withShardNameTemplate(shardTemplate));
    }

    /** See {@link TypedWrite#withSuffix}. */
    public Write<T> withSuffix(String filenameSuffix) {
      return new Write<>(inner.withSuffix(filenameSuffix));
    }

    /** See {@link TypedWrite#withNumShards}. */
    public Write<T> withNumShards(int numShards) {
      return new Write<>(inner.withNumShards(numShards));
    }

    /** See {@link TypedWrite#withoutSharding}. */
    public Write<T> withoutSharding() {
      return new Write<>(inner.withoutSharding());
    }

    /** See {@link TypedWrite#withWindowedWrites}. */
    public Write withWindowedWrites() {
      return new Write<T>(inner.withWindowedWrites());
    }

    /** See {@link TypedWrite#withCodec}. */
    public Write<T> withCodec(CodecFactory codec) {
      return new Write<>(inner.withCodec(codec));
    }

    /** See {@link TypedWrite#withMetadata} . */
    public Write withMetadata(Map<String, Object> metadata) {
      return new Write<>(inner.withMetadata(metadata));
    }

    @Override
    public PDone expand(PCollection<T> input) {
      return inner.expand(input);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }
  }

  /**
   * Returns a {@link DynamicAvroDestinations} that always returns the same {@link FilenamePolicy},
   * schema, metadata, and codec.
   */
  public static <UserT, OutputT> DynamicAvroDestinations<UserT, Void, OutputT> constantDestinations(
      FilenamePolicy filenamePolicy,
      Schema schema,
      Map<String, Object> metadata,
      CodecFactory codec,
      SerializableFunction<UserT, OutputT> formatFunction) {
    return new ConstantAvroDestination<>(filenamePolicy, schema, metadata, codec, formatFunction);
  }
  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private AvroIO() {}
}
