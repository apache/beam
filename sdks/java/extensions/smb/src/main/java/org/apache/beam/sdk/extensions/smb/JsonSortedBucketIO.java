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

import static org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * API for reading and writing BigQuery {@link TableRow} JSON sorted-bucket files.
 *
 * <h2>Reading BigQuery {@link TableRow} JSON sorted-bucket files</h2>
 *
 * <p>To read a {@link PCollection} from a single BigQuery {@link TableRow} JSON sorted-bucket
 * source, use {@link org.apache.beam.sdk.io.TextIO} followed by a {@link
 * org.apache.beam.sdk.transforms.MapElements} transform.
 *
 * <p>To read a {@code PCollection<KV<K, CoGbkResult>>} from multiple sorted-bucket sources, similar
 * to that of a {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, use {@link
 * SortedBucketIO.CoGbk} with {@link JsonSortedBucketIO.Read}. See {@link SortedBucketIO} for more
 * information.
 *
 * <h2>Writing BigQuery {@link TableRow} JSON sorted-bucket files</h2>
 *
 * <p>To write a {@link PCollection} to JSON sorted-bucket files, use {@link
 * JsonSortedBucketIO.Write}, using {@code JsonSortedBucketIO.write().to(String)} to specify the
 * output directory. Other options are similar to those of {@link
 * org.apache.beam.sdk.io.TextIO.Write}.
 */
public class JsonSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".json";

  /** Returns a new {@link Read} for BigQuery {@link TableRow} JSON records. */
  public static Read read(TupleTag<TableRow> tupleTag) {
    return new AutoValue_JsonSortedBucketIO_Read.Builder()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.AUTO)
        .build();
  }

  /** Returns a new {@link Write} for BigQuery {@link TableRow} JSON records. */
  public static <K> Write<K> write(Class<K> keyClass, String keyField) {
    return new AutoValue_JsonSortedBucketIO_Write.Builder<K>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.UNCOMPRESSED)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Reads from BigQuery {@link TableRow} JSON sorted-bucket files, to be used with {@link
   * SortedBucketIO.CoGbk}.
   */
  @AutoValue
  public abstract static class Read extends SortedBucketIO.Read<TableRow> {
    abstract TupleTag<TableRow> getTupleTag();

    @Nullable
    abstract ResourceId getInputDirectory();

    abstract String getFilenameSuffix();

    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTupleTag(TupleTag<TableRow> tupleTag);

      abstract Builder setInputDirectory(ResourceId inputDirectory);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setCompression(Compression compression);

      abstract Read build();
    }

    /** Reads from the given input directory. */
    public Read from(String inputDirectory) {
      return toBuilder()
          .setInputDirectory(FileSystems.matchNewResource(inputDirectory, true))
          .build();
    }

    /** Specifies the input filename suffix. */
    public Read withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the input file {@link Compression}. */
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    protected BucketedInput<?, TableRow> toBucketedInput() {
      return new BucketedInput<>(
          getTupleTag(),
          getInputDirectory(),
          getFilenameSuffix(),
          JsonFileOperations.of(getCompression()));
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Writes to BigQuery {@link TableRow} JSON sorted-bucket files using {@link SortedBucketSink}.
   */
  @AutoValue
  public abstract static class Write<K> extends PTransform<PCollection<TableRow>, WriteResult> {
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

    // JSON specific
    @Nullable
    abstract String getKeyField();

    abstract Compression getCompression();

    abstract Builder<K> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K> {
      // Common
      abstract Builder<K> setNumBuckets(int numBuckets);

      abstract Builder<K> setNumShards(int numShards);

      abstract Builder<K> setKeyClass(Class<K> keyClass);

      abstract Builder<K> setHashType(HashType hashType);

      abstract Builder<K> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K> setSorterMemoryMb(int sorterMemoryMb);

      // JSON specific
      abstract Builder<K> setKeyField(String keyField);

      abstract Builder<K> setCompression(Compression compression);

      abstract Write<K> build();
    }

    /** Specifies the number of buckets for partitioning. */
    public Write<K> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    /** Specifies the number of shards for partitioning. */
    public Write<K> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /** Specifies the {@link HashType} for partitioning. */
    public Write<K> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    /** Writes to the given output directory. */
    public Write<K> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    /** Specifies the temporary directory for writing. */
    public Write<K> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    /** Specifies the output filename suffix. */
    public Write<K> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /** Specifies the sorter memory in MB. */
    public Write<K> withSorterMemoryMb(int sorterMemoryMb) {
      return toBuilder().setSorterMemoryMb(sorterMemoryMb).build();
    }

    /** Specifies the output file {@link Compression}. */
    public Write<K> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    public WriteResult expand(PCollection<TableRow> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");

      BucketMetadata<K, TableRow> metadata;
      try {
        metadata =
            new JsonBucketMetadata<>(
                getNumBuckets(), getNumShards(), getKeyClass(), getHashType(), getKeyField());
      } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
        throw new IllegalStateException(e);
      }

      final ResourceId outputDirectory = getOutputDirectory();
      ResourceId tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }

      final JsonFileOperations fileOperations = JsonFileOperations.of(getCompression());
      SortedBucketSink<K, TableRow> sink =
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
