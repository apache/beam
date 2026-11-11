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
package org.apache.beam.sdk.io.iceberg.maintenance;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * A count of files per {@link FileCategory}.
 *
 * <p>Maintenance transforms emit partial counts that are summed with {@link #sum()} into the totals
 * reported by each maintenance operation.
 *
 * <p>Elements are encoded with a schema coder. The class is also {@link Serializable} because
 * {@code Combine.globally} captures the combine's default (empty-input) value, {@link #zeros()}, in
 * a Java-serialized DoFn.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
@Internal
public abstract class FileCategoryCounts implements Serializable {

  @SchemaFieldNumber("0")
  public abstract long getDataFiles();

  @SchemaFieldNumber("1")
  public abstract long getPositionDeleteFiles();

  @SchemaFieldNumber("2")
  public abstract long getEqualityDeleteFiles();

  @SchemaFieldNumber("3")
  public abstract long getManifests();

  @SchemaFieldNumber("4")
  public abstract long getManifestLists();

  @SchemaFieldNumber("5")
  public abstract long getStatisticsFiles();

  @SchemaFieldNumber("6")
  public abstract long getOtherMetadataFiles();

  @SchemaFieldNumber("7")
  public abstract long getUnknownFiles();

  /** Returns counts that are zero for every category. */
  public static FileCategoryCounts zeros() {
    return builder()
        .setDataFiles(0L)
        .setPositionDeleteFiles(0L)
        .setEqualityDeleteFiles(0L)
        .setManifests(0L)
        .setManifestLists(0L)
        .setStatisticsFiles(0L)
        .setOtherMetadataFiles(0L)
        .setUnknownFiles(0L)
        .build();
  }

  /** Returns counts with {@code count} files in {@code category} and zero elsewhere. */
  public static FileCategoryCounts of(FileCategory category, long count) {
    Builder builder = zeros().toBuilder();
    switch (category) {
      case DATA:
        return builder.setDataFiles(count).build();
      case POSITION_DELETES:
        return builder.setPositionDeleteFiles(count).build();
      case EQUALITY_DELETES:
        return builder.setEqualityDeleteFiles(count).build();
      case MANIFEST:
        return builder.setManifests(count).build();
      case MANIFEST_LIST:
        return builder.setManifestLists(count).build();
      case STATISTICS:
        return builder.setStatisticsFiles(count).build();
      case OTHER_METADATA:
        return builder.setOtherMetadataFiles(count).build();
      case UNKNOWN:
        return builder.setUnknownFiles(count).build();
    }
    throw new IllegalArgumentException("Unsupported file category: " + category);
  }

  /** Returns the number of files in {@code category}. */
  public long get(FileCategory category) {
    switch (category) {
      case DATA:
        return getDataFiles();
      case POSITION_DELETES:
        return getPositionDeleteFiles();
      case EQUALITY_DELETES:
        return getEqualityDeleteFiles();
      case MANIFEST:
        return getManifests();
      case MANIFEST_LIST:
        return getManifestLists();
      case STATISTICS:
        return getStatisticsFiles();
      case OTHER_METADATA:
        return getOtherMetadataFiles();
      case UNKNOWN:
        return getUnknownFiles();
    }
    throw new IllegalArgumentException("Unsupported file category: " + category);
  }

  /** Returns the number of files across all categories. */
  public long total() {
    long total = 0L;
    for (FileCategory category : FileCategory.values()) {
      total += get(category);
    }
    return total;
  }

  /** Returns the category-wise sum of these counts and {@code other}. */
  public FileCategoryCounts plus(FileCategoryCounts other) {
    return builder()
        .setDataFiles(getDataFiles() + other.getDataFiles())
        .setPositionDeleteFiles(getPositionDeleteFiles() + other.getPositionDeleteFiles())
        .setEqualityDeleteFiles(getEqualityDeleteFiles() + other.getEqualityDeleteFiles())
        .setManifests(getManifests() + other.getManifests())
        .setManifestLists(getManifestLists() + other.getManifestLists())
        .setStatisticsFiles(getStatisticsFiles() + other.getStatisticsFiles())
        .setOtherMetadataFiles(getOtherMetadataFiles() + other.getOtherMetadataFiles())
        .setUnknownFiles(getUnknownFiles() + other.getUnknownFiles())
        .build();
  }

  /**
   * Returns a {@link CombineFn} that sums counts category-wise.
   *
   * <p>The identity is {@link #zeros()}, so combining an empty input globally produces a single
   * all-zeros element.
   */
  public static CombineFn<FileCategoryCounts, FileCategoryCounts, FileCategoryCounts> sum() {
    return new SumFn();
  }

  static Builder builder() {
    return new AutoValue_FileCategoryCounts.Builder();
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setDataFiles(long count);

    abstract Builder setPositionDeleteFiles(long count);

    abstract Builder setEqualityDeleteFiles(long count);

    abstract Builder setManifests(long count);

    abstract Builder setManifestLists(long count);

    abstract Builder setStatisticsFiles(long count);

    abstract Builder setOtherMetadataFiles(long count);

    abstract Builder setUnknownFiles(long count);

    abstract FileCategoryCounts build();
  }

  private static class SumFn
      extends CombineFn<FileCategoryCounts, FileCategoryCounts, FileCategoryCounts> {

    @Override
    public FileCategoryCounts createAccumulator() {
      return zeros();
    }

    @Override
    public FileCategoryCounts addInput(FileCategoryCounts accumulator, FileCategoryCounts input) {
      return accumulator.plus(input);
    }

    @Override
    public FileCategoryCounts mergeAccumulators(Iterable<FileCategoryCounts> accumulators) {
      FileCategoryCounts merged = zeros();
      for (FileCategoryCounts accumulator : accumulators) {
        merged = merged.plus(accumulator);
      }
      return merged;
    }

    @Override
    public FileCategoryCounts extractOutput(FileCategoryCounts accumulator) {
      return accumulator;
    }

    // The accumulator and output have the same type as the input, so they share its coder.
    @Override
    public Coder<FileCategoryCounts> getAccumulatorCoder(
        CoderRegistry registry, Coder<FileCategoryCounts> inputCoder) {
      return inputCoder;
    }

    @Override
    public Coder<FileCategoryCounts> getDefaultOutputCoder(
        CoderRegistry registry, Coder<FileCategoryCounts> inputCoder) {
      return inputCoder;
    }
  }
}
