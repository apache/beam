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

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.joda.time.Duration;

/**
 * {@link PTransform}s for reading and writing Avro files.
 *
 * <h2>Reading Avro files</h2>
 *
 * <p>To read a {@link PCollection} from one or more Avro files with the same schema known at
 * pipeline construction time, use {@link #read}, using {@link AvroIO.Read#from} to specify the
 * filename or filepattern to read from. If the filepatterns to be read are themselves in a {@link
 * PCollection} you can use {@link FileIO} to match them and {@link AvroIO#readFiles} to read them.
 * If the schema is unknown at pipeline construction time, use {@link #parseGenericRecords} or
 * {@link #parseFilesGenericRecords}.
 *
 * <p>Many configuration options below apply to several or all of these transforms.
 *
 * <p>See {@link FileSystems} for information on supported file systems and filepatterns.
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, the filepatterns are expanded only once. {@link Read#watchForNewFiles} or the
 * combination of {@link FileIO.Match#continuously(Duration, TerminationCondition)} and {@link
 * AvroIO#readFiles(Class)} allow streaming of new files matching the filepattern(s).
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link
 * AvroIO#readFiles(Class)} allows them in case the filepattern contains a glob wildcard character.
 * Use {@link Read#withEmptyMatchTreatment} or {@link
 * FileIO.Match#withEmptyMatchTreatment(EmptyMatchTreatment)} plus {@link AvroIO#readFiles(Class)}
 * to configure this behavior.
 *
 * <h3>Reading records of a known schema</h3>
 *
 * <p>To read specific records, such as Avro-generated classes, use {@link #read(Class)}. To read
 * {@link GenericRecord GenericRecords}, use {@link #readGenericRecords(Schema)} which takes a
 * {@link Schema} object, or {@link #readGenericRecords(String)} which takes an Avro schema in a
 * JSON-encoded string form. An exception will be thrown if a record doesn't match the specified
 * schema. Likewise, to read a {@link PCollection} of filepatterns, apply {@link FileIO} matching
 * plus {@link #readFilesGenericRecords}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // Read Avro-generated classes from files on GCS
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.read(AvroAutoGenClass.class).from("gs://my_bucket/path/to/records-*.avro"));
 *
 * // Read GenericRecord's of the given schema from files on GCS
 * Schema schema = new Schema.Parser().parse(new File("schema.avsc"));
 * PCollection<GenericRecord> records =
 *     p.apply(AvroIO.readGenericRecords(schema)
 *                .from("gs://my_bucket/path/to/records-*.avro"));
 * }</pre>
 *
 * <h3>Reading records of an unknown schema</h3>
 *
 * <p>To read records from files whose schema is unknown at pipeline construction time or differs
 * between files, use {@link #parseGenericRecords} - in this case, you will need to specify a
 * parsing function for converting each {@link GenericRecord} into a value of your custom type.
 * Likewise, to read a {@link PCollection} of filepatterns with unknown schema, use {@link FileIO}
 * matching plus {@link #parseFilesGenericRecords(SerializableFunction)}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Pipeline p = ...;
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
 * <h3>Reading from a {@link PCollection} of filepatterns</h3>
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> filepatterns = p.apply(...);
 * PCollection<AvroAutoGenClass> records =
 *     filepatterns.apply(AvroIO.read(AvroAutoGenClass.class));
 * PCollection<AvroAutoGenClass> records =
 *     filepatterns
 *         .apply(FileIO.matchAll())
 *         .apply(FileIO.readMatches())
 *         .apply(AvroIO.readFiles(AvroAutoGenClass.class));
 * PCollection<GenericRecord> genericRecords =
 *     filepatterns.apply(AvroIO.readGenericRecords(schema));
 * PCollection<Foo> records =
 *     filepatterns
 *         .apply(FileIO.matchAll())
 *         .apply(FileIO.readMatches())
 *         .apply(AvroIO.parseFilesGenericRecords(new SerializableFunction...);
 * }</pre>
 *
 * <h3>Streaming new files matching a filepattern</h3>
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<AvroAutoGenClass> lines = p.apply(AvroIO
 *     .read(AvroAutoGenClass.class)
 *     .from("gs://my_bucket/path/to/records-*.avro")
 *     .watchForNewFiles(
 *       // Check for new files every minute
 *       Duration.standardMinutes(1),
 *       // Stop watching the filepattern if no new files appear within an hour
 *       afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Reading a very large number of files</h3>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link Read#withHintMatchesManyFiles} for better performance and
 * scalability. Note that it may decrease performance if the filepattern matches only a small number
 * of files.
 *
 * <h3>Inferring Beam schemas from Avro files</h3>
 *
 * <p>If you want to use SQL or schema based operations on an Avro-based PCollection, you must
 * configure the read transform to infer the Beam schema and automatically setup the Beam related
 * coders by doing:
 *
 * <pre>{@code
 * PCollection<AvroAutoGenClass> records =
 *     p.apply(AvroIO.read(...).from(...).withBeamSchemas(true);
 * }</pre>
 *
 * <h3>Inferring Beam schemas from Avro PCollections</h3>
 *
 * <p>If you created an Avro-based PCollection by other means e.g. reading records from Kafka or as
 * the output of another PTransform, you may be interested on making your PCollection schema-aware
 * so you can use the Schema-based APIs or Beam's SqlTransform.
 *
 * <p>If you are using Avro specific records (generated classes from an Avro schema), you can
 * register a schema provider for the specific Avro class to make any PCollection of these objects
 * schema-aware.
 *
 * <pre>{@code
 * pipeline.getSchemaRegistry().registerSchemaProvider(AvroAutoGenClass.class, AvroAutoGenClass.getClassSchema());
 * }</pre>
 *
 * You can also manually set an Avro-backed Schema coder for a PCollection using {@link
 * org.apache.beam.sdk.schemas.utils.AvroUtils#schemaCoder(Class, Schema)} to make it schema-aware.
 *
 * <pre>{@code
 * PCollection<AvroAutoGenClass> records = ...
 * AvroCoder<AvroAutoGenClass> coder = (AvroCoder<AvroAutoGenClass>) users.getCoder();
 * records.setCoder(AvroUtils.schemaCoder(coder.getType(), coder.getSchema()));
 * }</pre>
 *
 * <p>If you are using GenericRecords you may need to set a specific Beam schema coder for each
 * PCollection to match their internal Avro schema.
 *
 * <pre>{@code
 * org.apache.avro.Schema avroSchema = ...
 * PCollection<GenericRecord> records = ...
 * records.setCoder(AvroUtils.schemaCoder(avroSchema));
 * }</pre>
 *
 * <h2>Writing Avro files</h2>
 *
 * <p>To write a {@link PCollection} to one or more Avro files, use {@link AvroIO.Write}, using
 * {@code AvroIO.write().to(String)} to specify the output filename prefix. The default {@link
 * DefaultFilenamePolicy} will use this prefix, in conjunction with a {@link ShardNameTemplate} (set
 * via {@link Write#withShardNameTemplate(String)}) and optional filename suffix (set via {@link
 * Write#withSuffix(String)}, to generate output filenames in a sharded way. You can override this
 * default write filename policy using {@link Write#to(FileBasedSink.FilenamePolicy)} to specify a
 * custom file naming policy.
 *
 * <p>By default, {@link AvroIO.Write} produces output files that are compressed using the {@link
 * org.apache.avro.file.Codec CodecFactory.snappyCodec()}. This default can be changed or overridden
 * using {@link AvroIO.Write#withCodec}.
 *
 * <h3>Writing specific or generic records</h3>
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
 * <h3>Writing windowed or unbounded data</h3>
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner - {@link AvroIO.Write#withWindowedWrites()}
 * will cause windowing and triggering to be preserved. When producing windowed writes with a
 * streaming runner that supports triggers, the number of output shards must be set explicitly using
 * {@link AvroIO.Write#withNumShards(int)}; some runners may set this for you to a runner-chosen
 * value, so you may need not set it yourself. A {@link FileBasedSink.FilenamePolicy} must be set,
 * and unique windows and triggers must produce unique filenames.
 *
 * <h3>Writing data to multiple destinations</h3>
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
 * PCollectionView<Map<Integer, String>> userToSchemaMap = events.apply(
 *     "ComputePerUserSchemas", new ComputePerUserSchemas());
 * events.apply("WriteAvros", AvroIO.<Integer>writeCustomTypeToGenericRecords()
 *     .to(new UserDynamicAvroDestinations(userToSchemaMap)));
 * }</pre>
 */
public class AvroIO {
  /**
   * Reads records of the given type from an Avro file (or multiple Avro files matching a pattern).
   *
   * <p>The schema must be specified using one of the {@code withSchema} functions.
   */
  public static <T> Read<T> read(Class<T> recordClass) {
    return new AutoValue_AvroIO_Read.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .setInferBeamSchema(false)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
   * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
   *
   * <p>You can read {@link GenericRecord} by using {@code #readFiles(GenericRecord.class)} or
   * {@code #readFiles(new Schema.Parser().parse(schema))} if the schema is a String.
   */
  public static <T> ReadFiles<T> readFiles(Class<T> recordClass) {
    return new AutoValue_AvroIO_ReadFiles.Builder<T>()
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .setInferBeamSchema(false)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  /**
   * Like {@link #read}, but reads each filepattern in the input {@link PCollection}.
   *
   * @deprecated You can achieve The functionality of {@link #readAll} using {@link FileIO} matching
   *     plus {@link #readFiles(Class)}. This is the preferred method to make composition explicit.
   *     {@link ReadAll} will not receive upgrades and will be removed in a future version of Beam.
   */
  @Deprecated
  public static <T> ReadAll<T> readAll(Class<T> recordClass) {
    return new AutoValue_AvroIO_ReadAll.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setRecordClass(recordClass)
        .setSchema(ReflectData.get().getSchema(recordClass))
        .setInferBeamSchema(false)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  /** Reads Avro file(s) containing records of the specified schema. */
  public static Read<GenericRecord> readGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_Read.Builder<GenericRecord>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setInferBeamSchema(false)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #readGenericRecords(Schema)}, but for a {@link PCollection} of {@link
   * FileIO.ReadableFile}, for example, returned by {@link FileIO#readMatches}.
   */
  public static ReadFiles<GenericRecord> readFilesGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_ReadFiles.Builder<GenericRecord>()
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setInferBeamSchema(false)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  /**
   * Like {@link #readGenericRecords(Schema)}, but for a {@link PCollection} of {@link
   * FileIO.ReadableFile}, for example, returned by {@link FileIO#readMatches}.
   *
   * @deprecated You can achieve The functionality of {@link #readAllGenericRecords(Schema)} using
   *     {@link FileIO} matching plus {@link #readGenericRecords(Schema)}. This is the preferred
   *     method to make composition explicit. {@link ReadAll} will not receive upgrades and will be
   *     removed in a future version of Beam.
   */
  @Deprecated
  public static ReadAll<GenericRecord> readAllGenericRecords(Schema schema) {
    return new AutoValue_AvroIO_ReadAll.Builder<GenericRecord>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setRecordClass(GenericRecord.class)
        .setSchema(schema)
        .setInferBeamSchema(false)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  /**
   * Reads Avro file(s) containing records of the specified schema. The schema is specified as a
   * JSON-encoded string.
   */
  public static Read<GenericRecord> readGenericRecords(String schema) {
    return readGenericRecords(new Schema.Parser().parse(schema));
  }

  /** Like {@link #readGenericRecords(String)}, but for {@link FileIO.ReadableFile} collections. */
  public static ReadFiles<GenericRecord> readFilesGenericRecords(String schema) {
    return readFilesGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Like {@link #readGenericRecords(String)}, but reads each filepattern in the input {@link
   * PCollection}.
   *
   * @deprecated You can achieve The functionality of {@link #readAllGenericRecords(String)} using
   *     {@link FileIO} matching plus {@link #readGenericRecords(String)}. This is the preferred
   *     method to make composition explicit. {@link ReadAll} will not receive upgrades and will be
   *     removed in a future version of Beam.
   */
  @Deprecated
  public static ReadAll<GenericRecord> readAllGenericRecords(String schema) {
    return readAllGenericRecords(new Schema.Parser().parse(schema));
  }

  /**
   * Reads Avro file(s) containing records of an unspecified schema and converting each record to a
   * custom type.
   */
  public static <T> Parse<T> parseGenericRecords(SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_Parse.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setParseFn(parseFn)
        .setHintMatchesManyFiles(false)
        .build();
  }

  /**
   * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each {@link
   * FileIO.ReadableFile} in the input {@link PCollection}.
   */
  public static <T> ParseFiles<T> parseFilesGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_ParseFiles.Builder<T>()
        .setParseFn(parseFn)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .build();
  }

  /**
   * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each filepattern in the
   * input {@link PCollection}.
   *
   * @deprecated You can achieve The functionality of {@link
   *     #parseAllGenericRecords(SerializableFunction)} using {@link FileIO} matching plus {@link
   *     #parseFilesGenericRecords(SerializableFunction)} ()}. This is the preferred method to make
   *     composition explicit. {@link ParseAll} will not receive upgrades and will be removed in a
   *     future version of Beam.
   */
  @Deprecated
  public static <T> ParseAll<T> parseAllGenericRecords(
      SerializableFunction<GenericRecord, T> parseFn) {
    return new AutoValue_AvroIO_ParseAll.Builder<T>()
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .setParseFn(parseFn)
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
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
  public static <UserT, OutputT> TypedWrite<UserT, Void, OutputT> writeCustomType() {
    return AvroIO.<UserT, OutputT>defaultWriteBuilder().setGenericRecords(false).build();
  }

  /**
   * Similar to {@link #writeCustomType()}, but specialized for the case where the output type is
   * {@link GenericRecord}. A schema must be specified either in {@link
   * DynamicAvroDestinations#getSchema} or if not using dynamic destinations, by using {@link
   * TypedWrite#withSchema(Schema)}.
   */
  public static <UserT> TypedWrite<UserT, Void, GenericRecord> writeCustomTypeToGenericRecords() {
    return AvroIO.<UserT, GenericRecord>defaultWriteBuilder().setGenericRecords(true).build();
  }

  /**
   * Writes Avro records of the specified schema. The schema is specified as a JSON-encoded string.
   */
  public static Write<GenericRecord> writeGenericRecords(String schema) {
    return writeGenericRecords(new Schema.Parser().parse(schema));
  }

  private static <UserT, OutputT> TypedWrite.Builder<UserT, Void, OutputT> defaultWriteBuilder() {
    return new AutoValue_AvroIO_TypedWrite.Builder<UserT, Void, OutputT>()
        .setFilenameSuffix(null)
        .setShardTemplate(null)
        .setNumShards(0)
        .setCodec(TypedWrite.DEFAULT_SERIALIZABLE_CODEC)
        .setMetadata(ImmutableMap.of())
        .setWindowedWrites(false)
        .setNoSpilling(false);
  }

  private static <T> PCollection<T> setBeamSchema(
      PCollection<T> pc, Class<T> clazz, @Nullable Schema schema) {
    org.apache.beam.sdk.schemas.Schema beamSchema =
        org.apache.beam.sdk.schemas.utils.AvroUtils.getSchema(clazz, schema);
    if (beamSchema != null) {
      pc.setSchema(
          beamSchema,
          org.apache.beam.sdk.schemas.utils.AvroUtils.getToRowFunction(clazz, schema),
          org.apache.beam.sdk.schemas.utils.AvroUtils.getFromRowFunction(clazz));
    }
    return pc;
  }

  /**
   * 64MB is a reasonable value that allows to amortize the cost of opening files, but is not so
   * large as to exhaust a typical runner's maximum amount of output per ProcessElement call.
   */
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

  /** Implementation of {@link #read} and {@link #readGenericRecords}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract Schema getSchema();

    abstract boolean getInferBeamSchema();

    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);

      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setInferBeamSchema(boolean infer);

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

    /** Sets the {@link MatchConfiguration}. */
    public Read<T> withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    /** Configures whether or not a filepattern matching no files is allowed. */
    public Read<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * Continuously watches for new files matching the filepattern, polling it at the given
     * interval, until the given termination condition is reached. The returned {@link PCollection}
     * is unbounded.
     *
     * <p>This works only in runners supporting {@link Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Read<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
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

    /**
     * If set to true, a Beam schema will be inferred from the AVRO schema. This allows the output
     * to be used by SQL and by the schema-transform library.
     */
    @Experimental(Kind.SCHEMAS)
    public Read<T> withBeamSchemas(boolean withBeamSchemas) {
      return toBuilder().setInferBeamSchema(withBeamSchemas).build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<T> expand(PBegin input) {
      checkNotNull(getFilepattern(), "filepattern");
      checkNotNull(getSchema(), "schema");

      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        PCollection<T> read =
            input.apply(
                "Read",
                org.apache.beam.sdk.io.Read.from(
                    createSource(
                        getFilepattern(),
                        getMatchConfiguration().getEmptyMatchTreatment(),
                        getRecordClass(),
                        getSchema())));
        return getInferBeamSchema() ? setBeamSchema(read, getRecordClass(), getSchema()) : read;
      }

      // All other cases go through FileIO + ReadFiles
      ReadFiles<T> readFiles =
          (getRecordClass() == GenericRecord.class)
              ? (ReadFiles<T>) readFilesGenericRecords(getSchema())
              : readFiles(getRecordClass());
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              "Read Matches",
              FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply("Via ReadFiles", readFiles);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("inferBeamSchema", getInferBeamSchema())
                  .withLabel("Infer Beam Schema"))
          .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
          .addIfNotNull(DisplayData.item("recordClass", getRecordClass()).withLabel("Record Class"))
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .include("matchConfiguration", getMatchConfiguration());
    }

    @SuppressWarnings("unchecked")
    private static <T> AvroSource<T> createSource(
        ValueProvider<String> filepattern,
        EmptyMatchTreatment emptyMatchTreatment,
        Class<T> recordClass,
        Schema schema) {
      AvroSource<?> source =
          AvroSource.from(filepattern).withEmptyMatchTreatment(emptyMatchTreatment);
      return recordClass == GenericRecord.class
          ? (AvroSource<T>) source.withSchema(schema)
          : source.withSchema(recordClass);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles<T>
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {
    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract Schema getSchema();

    abstract long getDesiredBundleSizeBytes();

    abstract boolean getInferBeamSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder<T> setInferBeamSchema(boolean infer);

      abstract ReadFiles<T> build();
    }

    @VisibleForTesting
    ReadFiles<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    /**
     * If set to true, a Beam schema will be inferred from the AVRO schema. This allows the output
     * to be used by SQL and by the schema-transform library.
     */
    @Experimental(Kind.SCHEMAS)
    public ReadFiles<T> withBeamSchemas(boolean withBeamSchemas) {
      return toBuilder().setInferBeamSchema(withBeamSchemas).build();
    }

    @Override
    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
      checkNotNull(getSchema(), "schema");
      PCollection<T> read =
          input.apply(
              "Read all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  getDesiredBundleSizeBytes(),
                  new CreateSourceFn<>(getRecordClass(), getSchema().toString()),
                  AvroCoder.of(getRecordClass(), getSchema())));
      return getInferBeamSchema() ? setBeamSchema(read, getRecordClass(), getSchema()) : read;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("inferBeamSchema", getInferBeamSchema())
                  .withLabel("Infer Beam Schema"))
          .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
          .addIfNotNull(
              DisplayData.item("recordClass", getRecordClass()).withLabel("Record Class"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Implementation of {@link #readAll}.
   *
   * @deprecated See {@link #readAll(Class)} for details.
   */
  @Deprecated
  @AutoValue
  public abstract static class ReadAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    abstract MatchConfiguration getMatchConfiguration();

    @Nullable
    abstract Class<T> getRecordClass();

    @Nullable
    abstract Schema getSchema();

    abstract long getDesiredBundleSizeBytes();

    abstract boolean getInferBeamSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder<T> setInferBeamSchema(boolean infer);

      abstract ReadAll<T> build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public ReadAll<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public ReadAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public ReadAll<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
    }

    @VisibleForTesting
    ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    /**
     * If set to true, a Beam schema will be inferred from the AVRO schema. This allows the output
     * to be used by SQL and by the schema-transform library.
     */
    @Experimental(Kind.SCHEMAS)
    public ReadAll<T> withBeamSchemas(boolean withBeamSchemas) {
      return toBuilder().setInferBeamSchema(withBeamSchemas).build();
    }

    @Override
    public PCollection<T> expand(PCollection<String> input) {
      checkNotNull(getSchema(), "schema");
      PCollection<T> read =
          input
              .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
              .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
              .apply(readFiles(getRecordClass()));
      return getInferBeamSchema() ? setBeamSchema(read, getRecordClass(), getSchema()) : read;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("inferBeamSchema", getInferBeamSchema())
                  .withLabel("Infer Beam Schema"))
          .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
          .addIfNotNull(DisplayData.item("recordClass", getRecordClass()).withLabel("Record Class"))
          .include("matchConfiguration", getMatchConfiguration());
    }
  }

  private static class CreateSourceFn<T>
      implements SerializableFunction<String, FileBasedSource<T>> {
    private final Class<T> recordClass;
    private final Supplier<Schema> schemaSupplier;

    CreateSourceFn(Class<T> recordClass, String jsonSchema) {
      this.recordClass = recordClass;
      this.schemaSupplier =
          Suppliers.memoize(
              Suppliers.compose(new JsonToSchema(), Suppliers.ofInstance(jsonSchema)));
    }

    @Override
    public FileBasedSource<T> apply(String input) {
      return Read.createSource(
          StaticValueProvider.of(input),
          EmptyMatchTreatment.DISALLOW,
          recordClass,
          schemaSupplier.get());
    }

    private static class JsonToSchema implements Function<String, Schema>, Serializable {
      @Override
      public Schema apply(String input) {
        return new Schema.Parser().parse(input);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseGenericRecords}. */
  @AutoValue
  public abstract static class Parse<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract SerializableFunction<GenericRecord, T> getParseFn();

    @Nullable
    abstract Coder<T> getCoder();

    abstract boolean getHintMatchesManyFiles();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFilepattern(ValueProvider<String> filepattern);

      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);

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

    /** Sets the {@link MatchConfiguration}. */
    public Parse<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public Parse<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Parse<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
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

      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply(
            org.apache.beam.sdk.io.Read.from(
                AvroSource.from(getFilepattern()).withParseFn(getParseFn(), coder)));
      }

      // All other cases go through FileIO + ParseFilesGenericRecords.
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              "Read Matches",
              FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply("Via ParseFiles", parseFilesGenericRecords(getParseFn()).withCoder(coder));
    }

    private static <T> Coder<T> inferCoder(
        @Nullable Coder<T> explicitCoder,
        SerializableFunction<GenericRecord, T> parseFn,
        CoderRegistry coderRegistry) {
      if (explicitCoder != null) {
        return explicitCoder;
      }
      // If a coder was not specified explicitly, infer it from parse fn.
      try {
        return coderRegistry.getCoder(TypeDescriptors.outputOf(parseFn));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn. Specify it explicitly using withCoder().",
            e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"))
          .include("matchConfiguration", getMatchConfiguration());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #parseFilesGenericRecords}. */
  @AutoValue
  public abstract static class ParseFiles<T>
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {
    abstract SerializableFunction<GenericRecord, T> getParseFn();

    @Nullable
    abstract Coder<T> getCoder();

    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ParseFiles<T> build();
    }

    /** Specifies the coder for the result of the {@code parseFn}. */
    public ParseFiles<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @VisibleForTesting
    ParseFiles<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
      final Coder<T> coder =
          Parse.inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());
      final SerializableFunction<GenericRecord, T> parseFn = getParseFn();
      final SerializableFunction<String, FileBasedSource<T>> createSource =
          new CreateParseSourceFn<>(parseFn, coder);
      return input.apply(
          "Parse Files via FileBasedSource",
          new ReadAllViaFileBasedSource<>(getDesiredBundleSizeBytes(), createSource, coder));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
    }

    private static class CreateParseSourceFn<T>
        implements SerializableFunction<String, FileBasedSource<T>> {
      private final SerializableFunction<GenericRecord, T> parseFn;
      private final Coder<T> coder;

      CreateParseSourceFn(SerializableFunction<GenericRecord, T> parseFn, Coder<T> coder) {
        this.parseFn = parseFn;
        this.coder = coder;
      }

      @Override
      public FileBasedSource<T> apply(String input) {
        return AvroSource.from(input).withParseFn(parseFn, coder);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Implementation of {@link #parseAllGenericRecords}.
   *
   * @deprecated See {@link #parseAllGenericRecords(SerializableFunction)} for details.
   */
  @Deprecated
  @AutoValue
  public abstract static class ParseAll<T> extends PTransform<PCollection<String>, PCollection<T>> {
    abstract MatchConfiguration getMatchConfiguration();

    abstract SerializableFunction<GenericRecord, T> getParseFn();

    @Nullable
    abstract Coder<T> getCoder();

    abstract long getDesiredBundleSizeBytes();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ParseAll<T> build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public ParseAll<T> withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    /** Like {@link Read#withEmptyMatchTreatment}. */
    public ParseAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Read#watchForNewFiles}. */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public ParseAll<T> watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withMatchConfiguration(
          getMatchConfiguration().continuously(pollInterval, terminationCondition));
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
      return input
          .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(
              "Parse all via FileBasedSource",
              parseFilesGenericRecords(getParseFn()).withCoder(getCoder()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"))
          .include("matchConfiguration", getMatchConfiguration());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class TypedWrite<UserT, DestinationT, OutputT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
    static final CodecFactory DEFAULT_CODEC = CodecFactory.snappyCodec();
    static final SerializableAvroCodecFactory DEFAULT_SERIALIZABLE_CODEC =
        new SerializableAvroCodecFactory(DEFAULT_CODEC);

    @Nullable
    abstract SerializableFunction<UserT, OutputT> getFormatFunction();

    @Nullable
    abstract ValueProvider<ResourceId> getFilenamePrefix();

    @Nullable
    abstract String getShardTemplate();

    @Nullable
    abstract String getFilenameSuffix();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectory();

    abstract int getNumShards();

    abstract boolean getGenericRecords();

    @Nullable
    abstract Schema getSchema();

    abstract boolean getWindowedWrites();

    abstract boolean getNoSpilling();

    @Nullable
    abstract FilenamePolicy getFilenamePolicy();

    @Nullable
    abstract DynamicAvroDestinations<UserT, DestinationT, OutputT> getDynamicDestinations();

    /**
     * The codec used to encode the blocks in the Avro file. String value drawn from those in
     * https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/file/CodecFactory.html
     */
    abstract SerializableAvroCodecFactory getCodec();
    /** Avro file metadata. */
    abstract ImmutableMap<String, Object> getMetadata();

    abstract Builder<UserT, DestinationT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT, DestinationT, OutputT> {
      abstract Builder<UserT, DestinationT, OutputT> setFormatFunction(
          @Nullable SerializableFunction<UserT, OutputT> formatFunction);

      abstract Builder<UserT, DestinationT, OutputT> setFilenamePrefix(
          ValueProvider<ResourceId> filenamePrefix);

      abstract Builder<UserT, DestinationT, OutputT> setFilenameSuffix(
          @Nullable String filenameSuffix);

      abstract Builder<UserT, DestinationT, OutputT> setTempDirectory(
          ValueProvider<ResourceId> tempDirectory);

      abstract Builder<UserT, DestinationT, OutputT> setNumShards(int numShards);

      abstract Builder<UserT, DestinationT, OutputT> setShardTemplate(
          @Nullable String shardTemplate);

      abstract Builder<UserT, DestinationT, OutputT> setGenericRecords(boolean genericRecords);

      abstract Builder<UserT, DestinationT, OutputT> setSchema(Schema schema);

      abstract Builder<UserT, DestinationT, OutputT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT, DestinationT, OutputT> setNoSpilling(boolean noSpilling);

      abstract Builder<UserT, DestinationT, OutputT> setFilenamePolicy(
          FilenamePolicy filenamePolicy);

      abstract Builder<UserT, DestinationT, OutputT> setCodec(SerializableAvroCodecFactory codec);

      abstract Builder<UserT, DestinationT, OutputT> setMetadata(
          ImmutableMap<String, Object> metadata);

      abstract Builder<UserT, DestinationT, OutputT> setDynamicDestinations(
          DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations);

      abstract TypedWrite<UserT, DestinationT, OutputT> build();
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
    public TypedWrite<UserT, DestinationT, OutputT> to(String outputPrefix) {
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
    public TypedWrite<UserT, DestinationT, OutputT> to(ResourceId outputPrefix) {
      return toResource(StaticValueProvider.of(outputPrefix));
    }

    private static class OutputPrefixToResourceId
        implements SerializableFunction<String, ResourceId> {
      @Override
      public ResourceId apply(String input) {
        return FileBasedSink.convertToFileResourceIfPossible(input);
      }
    }

    /** Like {@link #to(String)}. */
    public TypedWrite<UserT, DestinationT, OutputT> to(ValueProvider<String> outputPrefix) {
      return toResource(
          NestedValueProvider.of(
              outputPrefix,
              // The function cannot be created as an anonymous class here since the enclosed class
              // may contain unserializable members.
              new OutputPrefixToResourceId()));
    }

    /** Like {@link #to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, DestinationT, OutputT> toResource(
        ValueProvider<ResourceId> outputPrefix) {
      return toBuilder().setFilenamePrefix(outputPrefix).build();
    }

    /**
     * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
     * directory for temporary files must be specified using {@link #withTempDirectory}.
     */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, DestinationT, OutputT> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Use a {@link DynamicAvroDestinations} object to vend {@link FilenamePolicy} objects. These
     * objects can examine the input record when creating a {@link FilenamePolicy}. A directory for
     * temporary files must be specified using {@link #withTempDirectory}.
     *
     * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} instead.
     */
    @Experimental(Kind.FILESYSTEM)
    @Deprecated
    public <NewDestinationT> TypedWrite<UserT, NewDestinationT, OutputT> to(
        DynamicAvroDestinations<UserT, NewDestinationT, OutputT> dynamicDestinations) {
      return toBuilder()
          .setDynamicDestinations((DynamicAvroDestinations) dynamicDestinations)
          .build();
    }

    /**
     * Sets the the output schema. Can only be used when the output type is {@link GenericRecord}
     * and when not using {@link #to(DynamicAvroDestinations)}.
     */
    public TypedWrite<UserT, DestinationT, OutputT> withSchema(Schema schema) {
      return toBuilder().setSchema(schema).build();
    }

    /**
     * Specifies a format function to convert {@link UserT} to the output type. If {@link
     * #to(DynamicAvroDestinations)} is used, {@link DynamicAvroDestinations#formatRecord} must be
     * used instead.
     */
    public TypedWrite<UserT, DestinationT, OutputT> withFormatFunction(
        @Nullable SerializableFunction<UserT, OutputT> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, DestinationT, OutputT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /** Set the base directory used to generate temporary files. */
    @Experimental(Kind.FILESYSTEM)
    public TypedWrite<UserT, DestinationT, OutputT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when using one of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT, DestinationT, OutputT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when using one
     * of the default filename-prefix to() overrides.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public TypedWrite<UserT, DestinationT, OutputT> withSuffix(String filenameSuffix) {
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
    public TypedWrite<UserT, DestinationT, OutputT> withNumShards(int numShards) {
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
    public TypedWrite<UserT, DestinationT, OutputT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     *
     * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
     * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
     */
    public TypedWrite<UserT, DestinationT, OutputT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    /** See {@link WriteFiles#withNoSpilling()}. */
    public TypedWrite<UserT, DestinationT, OutputT> withNoSpilling() {
      return toBuilder().setNoSpilling(true).build();
    }

    /** Writes to Avro file(s) compressed using specified codec. */
    public TypedWrite<UserT, DestinationT, OutputT> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(new SerializableAvroCodecFactory(codec)).build();
    }

    /**
     * Writes to Avro file(s) with the specified metadata.
     *
     * <p>Supported value types are String, Long, and byte[].
     */
    public TypedWrite<UserT, DestinationT, OutputT> withMetadata(Map<String, Object> metadata) {
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

    DynamicAvroDestinations<UserT, DestinationT, OutputT> resolveDynamicDestinations() {
      DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations =
          getDynamicDestinations();
      if (dynamicDestinations == null) {
        // In this case DestinationT is Void.
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
            (DynamicAvroDestinations<UserT, DestinationT, OutputT>)
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
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
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
      } else {
        checkArgument(
            getSchema() != null, "Unless using DynamicDestinations, .withSchema() is required.");
      }

      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<UserT, DestinationT, OutputT> write =
          WriteFiles.to(
              new AvroSink<>(tempDirectory, resolveDynamicDestinations(), getGenericRecords()));
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (getWindowedWrites()) {
        write = write.withWindowedWrites();
      }
      if (getNoSpilling()) {
        write = write.withNoSpilling();
      }
      return input.apply("Write", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      resolveDynamicDestinations().populateDisplayData(builder);
      builder
          .addIfNotDefault(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(
              DisplayData.item("tempDirectory", getTempDirectory())
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
    @VisibleForTesting final TypedWrite<T, ?, T> inner;

    Write(TypedWrite<T, ?, T> inner) {
      this.inner = inner;
    }

    /** See {@link TypedWrite#to(String)}. */
    public Write<T> to(String outputPrefix) {
      return new Write<>(
          inner
              .to(FileBasedSink.convertToFileResourceIfPossible(outputPrefix))
              .withFormatFunction(SerializableFunctions.identity()));
    }

    /** See {@link TypedWrite#to(ResourceId)} . */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> to(ResourceId outputPrefix) {
      return new Write<>(
          inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    /** See {@link TypedWrite#to(ValueProvider)}. */
    public Write<T> to(ValueProvider<String> outputPrefix) {
      return new Write<>(
          inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    /** See {@link TypedWrite#to(ResourceId)}. */
    @Experimental(Kind.FILESYSTEM)
    public Write<T> toResource(ValueProvider<ResourceId> outputPrefix) {
      return new Write<>(
          inner.toResource(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    /** See {@link TypedWrite#to(FilenamePolicy)}. */
    public Write<T> to(FilenamePolicy filenamePolicy) {
      return new Write<>(
          inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.identity()));
    }

    /**
     * See {@link TypedWrite#to(DynamicAvroDestinations)}.
     *
     * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} instead.
     */
    @Deprecated
    public Write<T> to(DynamicAvroDestinations<T, ?, T> dynamicDestinations) {
      return new Write<>(inner.to(dynamicDestinations).withFormatFunction(null));
    }

    /** See {@link TypedWrite#withSchema}. */
    public Write<T> withSchema(Schema schema) {
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
    public Write<T> withWindowedWrites() {
      return new Write<>(inner.withWindowedWrites());
    }

    /** See {@link TypedWrite#withCodec}. */
    public Write<T> withCodec(CodecFactory codec) {
      return new Write<>(inner.withCodec(codec));
    }

    /**
     * Specify that output filenames are wanted.
     *
     * <p>The nested {@link TypedWrite}transform always has access to output filenames, however due
     * to backwards-compatibility concerns, {@link Write} cannot return them. This method simply
     * returns the inner {@link TypedWrite} transform which has {@link WriteFilesResult} as its
     * output type, allowing access to output files.
     *
     * <p>The supplied {@code DestinationT} type must be: the same as that supplied in {@link
     * #to(DynamicAvroDestinations)} if that method was used, or {@code Void} otherwise.
     */
    public <DestinationT> TypedWrite<T, DestinationT, T> withOutputFilenames() {
      return (TypedWrite) inner;
    }

    /** See {@link TypedWrite#withMetadata} . */
    public Write<T> withMetadata(Map<String, Object> metadata) {
      return new Write<>(inner.withMetadata(metadata));
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(inner);
      return PDone.in(input.getPipeline());
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

  /**
   * Formats an element of a user type into a record with the given schema.
   *
   * @deprecated Users can achieve the same by providing this transform in a {@link
   *     org.apache.beam.sdk.transforms.ParDo} before using write in AvroIO {@link #write(Class)}.
   */
  @Deprecated
  public interface RecordFormatter<ElementT> extends Serializable {
    GenericRecord formatRecord(ElementT element, Schema schema);
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements of the given generated class, like {@link #write(Class)}.
   */
  public static <ElementT> Sink<ElementT> sink(final Class<ElementT> clazz) {
    return new AutoValue_AvroIO_Sink.Builder<ElementT>()
        .setJsonSchema(ReflectData.get().getSchema(clazz).toString())
        .setMetadata(ImmutableMap.of())
        .setCodec(TypedWrite.DEFAULT_SERIALIZABLE_CODEC)
        .build();
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements with a given (common) schema, like {@link #writeGenericRecords(Schema)}.
   */
  @Experimental
  public static <ElementT extends IndexedRecord> Sink<ElementT> sink(Schema schema) {
    return sink(schema.toString());
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements with a given (common) schema, like {@link #writeGenericRecords(String)}.
   */
  @Experimental
  public static <ElementT extends IndexedRecord> Sink<ElementT> sink(String jsonSchema) {
    return new AutoValue_AvroIO_Sink.Builder<ElementT>()
        .setJsonSchema(jsonSchema)
        .setMetadata(ImmutableMap.of())
        .setCodec(TypedWrite.DEFAULT_SERIALIZABLE_CODEC)
        .build();
  }

  /**
   * A {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}, writing
   * elements by converting each one to a {@link GenericRecord} with a given (common) schema, like
   * {@link #writeCustomTypeToGenericRecords()}.
   *
   * @deprecated RecordFormatter will be removed in future versions.
   */
  @Deprecated
  public static <ElementT> Sink<ElementT> sinkViaGenericRecords(
      Schema schema, RecordFormatter<ElementT> formatter) {
    return new AutoValue_AvroIO_Sink.Builder<ElementT>()
        .setRecordFormatter(formatter)
        .setJsonSchema(schema.toString())
        .setMetadata(ImmutableMap.of())
        .setCodec(TypedWrite.DEFAULT_SERIALIZABLE_CODEC)
        .build();
  }

  /** Implementation of {@link #sink} and {@link #sinkViaGenericRecords}. */
  @AutoValue
  public abstract static class Sink<ElementT> implements FileIO.Sink<ElementT> {
    /** @deprecated RecordFormatter will be removed in future versions. */
    @Nullable
    @Deprecated
    abstract RecordFormatter<ElementT> getRecordFormatter();

    @Nullable
    abstract String getJsonSchema();

    abstract Map<String, Object> getMetadata();

    abstract SerializableAvroCodecFactory getCodec();

    abstract Builder<ElementT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ElementT> {
      /** @deprecated RecordFormatter will be removed in future versions. */
      @Deprecated
      abstract Builder<ElementT> setRecordFormatter(RecordFormatter<ElementT> formatter);

      abstract Builder<ElementT> setJsonSchema(String jsonSchema);

      abstract Builder<ElementT> setMetadata(Map<String, Object> metadata);

      abstract Builder<ElementT> setCodec(SerializableAvroCodecFactory codec);

      abstract Sink<ElementT> build();
    }

    /** Specifies to put the given metadata into each generated file. By default, empty. */
    public Sink<ElementT> withMetadata(Map<String, Object> metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    /**
     * Specifies to use the given {@link CodecFactory} for each generated file. By default, {@code
     * CodecFactory.snappyCodec()}.
     */
    public Sink<ElementT> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(new SerializableAvroCodecFactory(codec)).build();
    }

    @Nullable private transient Schema schema;
    @Nullable private transient DataFileWriter<ElementT> reflectWriter;
    @Nullable private transient DataFileWriter<GenericRecord> genericWriter;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.schema = new Schema.Parser().parse(getJsonSchema());
      DataFileWriter<?> writer;
      if (getRecordFormatter() == null) {
        writer = reflectWriter = new DataFileWriter<>(new ReflectDatumWriter<>(schema));
      } else {
        writer = genericWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));
      }
      writer.setCodec(getCodec().getCodec());
      for (Map.Entry<String, Object> entry : getMetadata().entrySet()) {
        Object v = entry.getValue();
        if (v instanceof String) {
          writer.setMeta(entry.getKey(), (String) v);
        } else if (v instanceof Long) {
          writer.setMeta(entry.getKey(), (Long) v);
        } else if (v instanceof byte[]) {
          writer.setMeta(entry.getKey(), (byte[]) v);
        } else {
          throw new IllegalStateException(
              "Metadata value type must be one of String, Long, or byte[]. Found "
                  + v.getClass().getSimpleName());
        }
      }
      writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ElementT element) throws IOException {
      if (getRecordFormatter() == null) {
        reflectWriter.append(element);
      } else {
        genericWriter.append(getRecordFormatter().formatRecord(element, schema));
      }
    }

    @Override
    public void flush() throws IOException {
      MoreObjects.firstNonNull(reflectWriter, genericWriter).flush();
    }
  }

  /** Disallow construction of utility class. */
  private AvroIO() {}
}
