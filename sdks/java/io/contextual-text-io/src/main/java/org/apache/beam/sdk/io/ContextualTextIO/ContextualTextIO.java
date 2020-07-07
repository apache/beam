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
package org.apache.beam.sdk.io.ContextualTextIO;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;

/** */
public class ContextualTextIO {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded {@link
   * PCollection} containing one {@link LineContext}element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_ContextualTextIO_Read.Builder()
        .setCompression(Compression.AUTO)
        .setHintMatchesManyFiles(false)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setHasRFC4180MultiLineColumn(false)
        .build();
  }

  /**
   * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
   * org.apache.beam.sdk.io.FileIO.ReadableFile}, returned by {@link
   * org.apache.beam.sdk.io.FileIO#readMatches}.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_ContextualTextIO_ReadFiles.Builder()
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .setHasRFC4180MultiLineColumn(false)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<LineContext>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract boolean getHintMatchesManyFiles();

    abstract Compression getCompression();

    @Nullable
    abstract Boolean getHasRFC4180MultiLineColumn();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    @Nullable
    abstract byte[] getDelimiter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Builder setCompression(Compression compression);

      abstract Builder setDelimiter(byte[] delimiter);

      abstract Builder setHasRFC4180MultiLineColumn(Boolean hasRFC4180MultiLineColumn);

      abstract Read build();
    }

    /**
     * Reads text files that reads from the file(s) with the given filename or filename pattern.
     *
     * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
     * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
     * remote execution service).
     *
     * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     *
     * <p>If it is known that the filepattern will match a very large number of files (at least tens
     * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
     */
    public Read from(String filepattern) {
      Preconditions.checkArgument(filepattern != null, "filepattern can not be null");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      Preconditions.checkArgument(filepattern != null, "filepattern can not be null");
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    /** Sets if the file has RFC4180 MultiLineColumn. */
    public Read withRFC4180MultiLineColumn(Boolean hasRFC4180MultiLineColumn) {
      return toBuilder().setHasRFC4180MultiLineColumn(hasRFC4180MultiLineColumn).build();
    }

    /**
     * Reads from input sources using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
     */
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    /**
     * See {@link MatchConfiguration#continuously}.
     *
     * <p>This works only in runners supporting {@link Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Kind.SPLITTABLE_DO_FN)
    public Read watchForNewFiles(
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
    public Read withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment}. */
    public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n'). */
    public Read withDelimiter(byte[] delimiter) {
      Preconditions.checkArgument(delimiter != null, "delimiter can not be null");
      Preconditions.checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
      return toBuilder().setDelimiter(delimiter).build();
    }

    /** Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n'). */
    public Read withHasRFC4180MultiLineColumn(boolean hasRFC4180MultiLineColumn) {
      return toBuilder().setHasRFC4180MultiLineColumn(hasRFC4180MultiLineColumn).build();
    }

    static boolean isSelfOverlapping(byte[] s) {
      // s self-overlaps if v exists such as s = vu = wv with u and w non empty
      for (int i = 1; i < s.length - 1; ++i) {
        if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
          return true;
        }
      }
      return false;
    }

    @Override
    public PCollection<LineContext> expand(PBegin input) {
      Preconditions.checkNotNull(
          getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
      }

      // All other cases go through FileIO + ReadFiles
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              "Read Matches",
              FileIO.readMatches()
                  .withCompression(getCompression())
                  .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply("Via ReadFiles", readFiles().withDelimiter(getDelimiter()));
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<LineContext> getSource() {
      return CompressedSource.from(
              new ContextualTextIOSource(
                  getFilepattern(),
                  getMatchConfiguration().getEmptyMatchTreatment(),
                  getDelimiter(),
                  getHasRFC4180MultiLineColumn()))
          .withCompression(getCompression());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotNull(DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
          .include("matchConfiguration", getMatchConfiguration())
          .addIfNotNull(
              DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                  .withLabel("Custom delimiter to split records"))
          .addIfNotNull(
              DisplayData.item("hasRFC4180MultiLineColumn", getHasRFC4180MultiLineColumn())
                  .withLabel("Has RFC4180 MultiLineColumn"));
    }
  }

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<LineContext>> {
    abstract long getDesiredBundleSizeBytes();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    @Nullable
    abstract byte[] getDelimiter();

    abstract boolean getHasRFC4180MultiLineColumn();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder setHasRFC4180MultiLineColumn(boolean hasRFC4180MultiLineColumn);

      abstract Builder setDelimiter(byte[] delimiter);

      abstract ReadFiles build();
    }

    @VisibleForTesting
    ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    /** Like {@link Read#withDelimiter}. */
    public ReadFiles withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    /** Like {@link Read#withDelimiter}. */
    public ReadFiles with(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    @Override
    public PCollection<LineContext> expand(PCollection<FileIO.ReadableFile> input) {

      SchemaCoder<LineContext> coder = null;

      try {
        coder = input.getPipeline().getSchemaRegistry().getSchemaCoder(LineContext.class);
      } catch (NoSuchSchemaException e) {
        System.out.println("No Coder!");
      }

      if(getHasRFC4180MultiLineColumn()) {
      }

      return input.apply(
          "Read all via FileBasedSource",
          new ReadAllViaFileBasedSource<>(
              getDesiredBundleSizeBytes(),
              new CreateTextSourceFn(getDelimiter(), getHasRFC4180MultiLineColumn()),
              coder));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
              .withLabel("Custom delimiter to split records"));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<LineContext>> {
      private byte[] delimiter;
      private boolean hasRFC4180MultiLineColumn;

      private CreateTextSourceFn(byte[] delimiter, boolean hasRFC4180MultiLineColumn) {
        this.delimiter = delimiter;
        this.hasRFC4180MultiLineColumn = hasRFC4180MultiLineColumn;
      }

      @Override
      public FileBasedSource<LineContext> apply(String input) {
        return new ContextualTextIOSource(
            StaticValueProvider.of(input),
            EmptyMatchTreatment.DISALLOW,
            delimiter,
            hasRFC4180MultiLineColumn);
      }
    }
  }

  /** Disable construction of utility class. */
  private ContextualTextIO() {}
}
