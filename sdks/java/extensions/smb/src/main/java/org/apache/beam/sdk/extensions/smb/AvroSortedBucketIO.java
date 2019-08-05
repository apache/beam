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
package org.apache.beam.sdk.extensions.smb;

import static org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * API for reading and writing Avro sorted-bucket files.
 *
 * <h2>Reading Avro sorted-bucket files</h2>
 *
 * <p>To read a {@link PCollection} from a single Avro sorted-bucket source, use {@link
 * org.apache.beam.sdk.io.AvroIO}.
 *
 * <p>To read a {@code PCollection<KV<K, CoGbkResult>>} from multiple sorted-bucket sources, similar
 * to that of a {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, use {@link
 * SortedBucketIO.CoGbk} with {@link AvroSortedBucketIO.Read}. See {@link SortedBucketIO} for more
 * information.
 *
 * <h2>Writing Avro sorted-bucket files</h2>
 *
 * <p>To write a {@link PCollection} to Avro sorted-bucket files, use {@link
 * AvroSortedBucketIO.Write}, using {@code AvroSortedBucketIO.write().to(String)} to specify the
 * output directory. Other options are similar to those of {@link
 * org.apache.beam.sdk.io.AvroIO.Write}.
 */
public class AvroSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".avro";
  private static final CodecFactory DEFAULT_CODEC = CodecFactory.snappyCodec();

  /** Returns a new {@link Read} for Avro generic records. */
  public static Read<GenericRecord> read(TupleTag<GenericRecord> tupleTag, Schema schema) {
    return new AutoValue_AvroSortedBucketIO_Read.Builder<>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(DEFAULT_CODEC)
        .setSchema(schema)
        .build();
  }

  /** Returns a new {@link Read} for Avro specific records. */
  public static <T extends SpecificRecordBase> Read<T> read(
      TupleTag<T> tupleTag, Class<T> recordClass) {
    return new AutoValue_AvroSortedBucketIO_Read.Builder<T>()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(DEFAULT_CODEC)
        .setRecordClass(recordClass)
        .build();
  }

  /** Returns a new {@link Write} for Avro generic records. */
  public static <K> Write<K, GenericRecord> write(
      Class<K> keyClass, String keyField, Schema schema) {
    return AvroSortedBucketIO.newBuilder(keyClass, keyField).setSchema(schema).build();
  }

  /** Returns a new {@link Write} for Avro specific records. */
  public static <K, T extends SpecificRecordBase> Write<K, T> write(
      Class<K> keyClass, String keyField, Class<T> recordClass) {
    return AvroSortedBucketIO.<K, T>newBuilder(keyClass, keyField)
        .setRecordClass(recordClass)
        .build();
  }

  private static <K, T extends GenericRecord> Write.Builder<K, T> newBuilder(
      Class<K> keyClass, String keyField) {
    return new AutoValue_AvroSortedBucketIO_Write.Builder<K, T>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(DEFAULT_CODEC);
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  /** Reads from Avro sorted-bucket files, to be used with {@link SortedBucketIO.CoGbk}. */
  @AutoValue
  public abstract static class Read<T extends GenericRecord> extends SortedBucketIO.Read<T> {
    abstract TupleTag<T> getTupleTag();

    @Nullable
    abstract ResourceId getInputDirectory();

    abstract String getFilenameSuffix();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T extends GenericRecord> {
      abstract Builder<T> setTupleTag(TupleTag<T> tupleTag);

      abstract Builder<T> setInputDirectory(ResourceId inputDirectory);

      abstract Builder<T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<T> setSchema(Schema schema);

      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract Builder<T> setCodec(CodecFactory codec);

      abstract Read<T> build();
    }

    /** Reads from the given input directory. */
    public Read<T> from(String inputDirectory) {
      return toBuilder()
          .setInputDirectory(FileSystems.matchNewResource(inputDirectory, true))
          .build();
    }

    /** Specifies the input filename suffix. */
    public Read<T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the input file {@link CodecFactory}. */
    public Read withCodec(CodecFactory codec) {
      return toBuilder().setCodec(codec).build();
    }

    @Override
    protected BucketedInput<?, T> toBucketedInput() {
      @SuppressWarnings("unchecked")
      final AvroFileOperations<T> fileOperations =
          getRecordClass() == null
              ? AvroFileOperations.of(getSchema(), getCodec())
              : (AvroFileOperations<T>)
                  AvroFileOperations.of((Class<SpecificRecordBase>) getRecordClass(), getCodec());
      return new BucketedInput<>(
          getTupleTag(), getInputDirectory(), getFilenameSuffix(), fileOperations);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  /** Writes to Avro sorted-bucket files using {@link SortedBucketSink}. */
  @AutoValue
  public abstract static class Write<K, T extends GenericRecord>
      extends PTransform<PCollection<T>, WriteResult> {
    // Common
    abstract int getNumBuckets();

    abstract int getNumShards();

    abstract Class<K> getKeyClass();

    abstract HashType getHashType();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract int getSorterMemoryMb();

    // Avro
    @Nullable
    abstract String getKeyField();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

    abstract Builder<K, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, T extends GenericRecord> {
      // Common
      abstract Builder<K, T> setNumBuckets(int numBuckets);

      abstract Builder<K, T> setNumShards(int numShards);

      abstract Builder<K, T> setKeyClass(Class<K> keyClass);

      abstract Builder<K, T> setHashType(HashType hashType);

      abstract Builder<K, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K, T> setSorterMemoryMb(int sorterMemoryMb);

      // Avro specific
      abstract Builder<K, T> setKeyField(String keyField);

      abstract Builder<K, T> setSchema(Schema schema);

      abstract Builder<K, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K, T> setCodec(CodecFactory codec);

      abstract Write<K, T> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K, T> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K, T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K, T> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. */
    public Write<K, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public Write<K, T> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K, T> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the output file {@link CodecFactory}. */
    public Write<K, T> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(codec).build();
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");

      BucketMetadata<K, T> metadata;
      try {
        metadata =
            new AvroBucketMetadata<>(
                getNumBuckets(), getNumShards(), getKeyClass(), getHashType(), getKeyField());
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }

      final ResourceId outputDirectory = getOutputDirectory();
      ResourceId tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }

      @SuppressWarnings("unchecked")
      final AvroFileOperations<T> fileOperations =
          getRecordClass() == null
              ? AvroFileOperations.of(getSchema(), getCodec())
              : (AvroFileOperations<T>)
                  AvroFileOperations.of((Class<SpecificRecordBase>) getRecordClass(), getCodec());
      SortedBucketSink<K, T> sink =
          new SortedBucketSink<>(
              metadata,
              outputDirectory,
              tempDirectory,
              getFilenameSuffix(),
              fileOperations,
              getSorterMemoryMb());
      return input.apply(sink);
    }
  }
}
