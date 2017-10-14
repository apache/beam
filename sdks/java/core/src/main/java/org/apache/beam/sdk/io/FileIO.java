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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms for working with files. Currently includes matching of filepatterns via {@link #match}
 * and {@link #matchAll}, and reading matches via {@link #readMatches}.
 */
public class FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(FileIO.class);

  /**
   * Matches a filepattern using {@link FileSystems#match} and produces a collection of matched
   * resources (both files and directories) as {@link MatchResult.Metadata}.
   *
   * <p>By default, matches the filepattern once and produces a bounded {@link PCollection}. To
   * continuously watch the filepattern for new matches, use {@link MatchAll#continuously(Duration,
   * TerminationCondition)} - this will produce an unbounded {@link PCollection}.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#DISALLOW}. To configure this behavior, use {@link
   * Match#withEmptyMatchTreatment}.
   */
  public static Match match() {
    return new AutoValue_FileIO_Match.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * Like {@link #match}, but matches each filepattern in a collection of filepatterns.
   *
   * <p>Resources are not deduplicated between filepatterns, i.e. if the same resource matches
   * multiple filepatterns, it will be produced multiple times.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#ALLOW_IF_WILDCARD}. To configure this behavior, use {@link
   * MatchAll#withEmptyMatchTreatment}.
   */
  public static MatchAll matchAll() {
    return new AutoValue_FileIO_MatchAll.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  /**
   * Converts each result of {@link #match} or {@link #matchAll} to a {@link ReadableFile} which can
   * be used to read the contents of each file, optionally decompressing it.
   */
  public static ReadMatches readMatches() {
    return new AutoValue_FileIO_ReadMatches.Builder()
        .setCompression(Compression.AUTO)
        .setDirectoryTreatment(ReadMatches.DirectoryTreatment.SKIP)
        .build();
  }

  /** A utility class for accessing a potentially compressed file. */
  public static final class ReadableFile {
    private final MatchResult.Metadata metadata;
    private final Compression compression;

    ReadableFile(MatchResult.Metadata metadata, Compression compression) {
      this.metadata = metadata;
      this.compression = compression;
    }

    /** Returns the {@link MatchResult.Metadata} of the file. */
    public MatchResult.Metadata getMetadata() {
      return metadata;
    }

    /** Returns the method with which this file will be decompressed in {@link #open}. */
    public Compression getCompression() {
      return compression;
    }

    /**
     * Returns a {@link ReadableByteChannel} reading the data from this file, potentially
     * decompressing it using {@link #getCompression}.
     */
    public ReadableByteChannel open() throws IOException {
      return compression.readDecompressed(FileSystems.open(metadata.resourceId()));
    }

    /**
     * Returns a {@link SeekableByteChannel} equivalent to {@link #open}, but fails if this file is
     * not {@link MatchResult.Metadata#isReadSeekEfficient seekable}.
     */
    public SeekableByteChannel openSeekable() throws IOException {
      checkState(
          getMetadata().isReadSeekEfficient(),
          "The file %s is not seekable",
          metadata.resourceId());
      return ((SeekableByteChannel) open());
    }

    /** Returns the full contents of the file as bytes. */
    public byte[] readFullyAsBytes() throws IOException {
      return StreamUtils.getBytes(Channels.newInputStream(open()));
    }

    /** Returns the full contents of the file as a {@link String} decoded as UTF-8. */
    public String readFullyAsUTF8String() throws IOException {
      return new String(readFullyAsBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
      return "ReadableFile{metadata=" + metadata + ", compression=" + compression + '}';
    }
  }

  /**
   * Describes configuration for matching filepatterns, such as {@link EmptyMatchTreatment} and
   * continuous watching for matching files.
   */
  @AutoValue
  public abstract static class MatchConfiguration implements HasDisplayData, Serializable {
    /** Creates a {@link MatchConfiguration} with the given {@link EmptyMatchTreatment}. */
    public static MatchConfiguration create(EmptyMatchTreatment emptyMatchTreatment) {
      return new AutoValue_FileIO_MatchConfiguration.Builder()
          .setEmptyMatchTreatment(emptyMatchTreatment)
          .build();
    }

    abstract EmptyMatchTreatment getEmptyMatchTreatment();

    @Nullable
    abstract Duration getWatchInterval();

    @Nullable
    abstract TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setWatchInterval(Duration watchInterval);

      abstract Builder setWatchTerminationCondition(TerminationCondition<String, ?> condition);

      abstract MatchConfiguration build();
    }

    /** Sets the {@link EmptyMatchTreatment}. */
    public MatchConfiguration withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    /**
     * Continuously watches for new files at the given interval until the given termination
     * condition is reached, where the input to the condition is the filepattern.
     */
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition) {
      return toBuilder().setWatchInterval(interval).setWatchTerminationCondition(condition).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder
          .add(
              DisplayData.item("emptyMatchTreatment", getEmptyMatchTreatment().toString())
                  .withLabel("Treatment of filepatterns that match no files"))
          .addIfNotNull(
              DisplayData.item("watchForNewFilesInterval", getWatchInterval())
                  .withLabel("Interval to watch for new files"));
    }
  }

  /** Implementation of {@link #match}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract Match build();
    }

    /** Matches the given filepattern. */
    public Match filepattern(String filepattern) {
      return this.filepattern(ValueProvider.StaticValueProvider.of(filepattern));
    }

    /** Like {@link #filepattern(String)} but using a {@link ValueProvider}. */
    public Match filepattern(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Match withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment(EmptyMatchTreatment)}. */
    public Match withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * See {@link MatchConfiguration#continuously}. The returned {@link PCollection} is unbounded.
     *
     * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via MatchAll", matchAll().withConfiguration(getConfiguration()));
    }
  }

  /** Implementation of {@link #matchAll}. */
  @AutoValue
  public abstract static class MatchAll
      extends PTransform<PCollection<String>, PCollection<MatchResult.Metadata>> {
    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract MatchAll build();
    }

    /** Like {@link Match#withConfiguration}. */
    public MatchAll withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** Like {@link Match#withEmptyMatchTreatment}. */
    public MatchAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Match#continuously}. */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input) {
      PCollection<MatchResult.Metadata> res;
      if (getConfiguration().getWatchInterval() == null) {
        res = input.apply(
            "Match filepatterns",
            ParDo.of(new MatchFn(getConfiguration().getEmptyMatchTreatment())));
      } else {
        res = input
            .apply(
                "Continuously match filepatterns",
                Watch.growthOf(new MatchPollFn())
                    .withPollInterval(getConfiguration().getWatchInterval())
                    .withTerminationPerInput(getConfiguration().getWatchTerminationCondition()))
            .apply(Values.<MatchResult.Metadata>create());
      }
      return res.apply(Reshuffle.<MatchResult.Metadata>viaRandomKey());
    }

    private static class MatchFn extends DoFn<String, MatchResult.Metadata> {
      private final EmptyMatchTreatment emptyMatchTreatment;

      public MatchFn(EmptyMatchTreatment emptyMatchTreatment) {
        this.emptyMatchTreatment = emptyMatchTreatment;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        String filepattern = c.element();
        MatchResult match = FileSystems.match(filepattern, emptyMatchTreatment);
        LOG.info("Matched {} files for pattern {}", match.metadata().size(), filepattern);
        for (MatchResult.Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class MatchPollFn extends Watch.Growth.PollFn<String, MatchResult.Metadata> {
      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String element, Context c)
          throws Exception {
        return Watch.Growth.PollResult.incomplete(
            Instant.now(), FileSystems.match(element, EmptyMatchTreatment.ALLOW).metadata());
      }
    }
  }

  /** Implementation of {@link #readMatches}. */
  @AutoValue
  public abstract static class ReadMatches
      extends PTransform<PCollection<MatchResult.Metadata>, PCollection<ReadableFile>> {
    enum DirectoryTreatment {
      SKIP,
      PROHIBIT
    }

    abstract Compression getCompression();

    abstract DirectoryTreatment getDirectoryTreatment();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompression(Compression compression);

      abstract Builder setDirectoryTreatment(DirectoryTreatment directoryTreatment);

      abstract ReadMatches build();
    }

    /** Reads files using the given {@link Compression}. Default is {@link Compression#AUTO}. */
    public ReadMatches withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      return toBuilder().setCompression(compression).build();
    }

    /**
     * Controls how to handle directories in the input {@link PCollection}. Default is {@link
     * DirectoryTreatment#SKIP}.
     */
    public ReadMatches withDirectoryTreatment(DirectoryTreatment directoryTreatment) {
      checkArgument(directoryTreatment != null, "directoryTreatment can not be null");
      return toBuilder().setDirectoryTreatment(directoryTreatment).build();
    }

    @Override
    public PCollection<ReadableFile> expand(PCollection<MatchResult.Metadata> input) {
      return input.apply(ParDo.of(new ToReadableFileFn(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("compression", getCompression().toString()));
      builder.add(DisplayData.item("directoryTreatment", getDirectoryTreatment().toString()));
    }

    private static class ToReadableFileFn extends DoFn<MatchResult.Metadata, ReadableFile> {
      private final ReadMatches spec;

      private ToReadableFileFn(ReadMatches spec) {
        this.spec = spec;
      }

      @ProcessElement
      public void process(ProcessContext c) {
        MatchResult.Metadata metadata = c.element();
        if (metadata.resourceId().isDirectory()) {
          switch (spec.getDirectoryTreatment()) {
            case SKIP:
              return;

            case PROHIBIT:
              throw new IllegalArgumentException(
                  "Trying to read " + metadata.resourceId() + " which is a directory");

            default:
              throw new UnsupportedOperationException(
                  "Unknown DirectoryTreatment: " + spec.getDirectoryTreatment());
          }
        }

        Compression compression =
            (spec.getCompression() == Compression.AUTO)
                ? Compression.detect(metadata.resourceId().getFilename())
                : spec.getCompression();
        c.output(
            new ReadableFile(
                MatchResult.Metadata.builder()
                    .setResourceId(metadata.resourceId())
                    .setSizeBytes(metadata.sizeBytes())
                    .setIsReadSeekEfficient(
                        metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
                    .build(),
                compression));
      }
    }
  }
}
