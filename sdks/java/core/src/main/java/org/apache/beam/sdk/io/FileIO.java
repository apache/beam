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

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MetadataCoderV2;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General-purpose transforms for working with files: listing files (matching), reading and writing.
 *
 * <h2>Matching filepatterns</h2>
 *
 * <p>{@link #match} and {@link #matchAll} match filepatterns (respectively either a single
 * filepattern or a {@link PCollection} thereof) and return the files that match them as {@link
 * PCollection PCollections} of {@link MatchResult.Metadata}. Configuration options for them are in
 * {@link MatchConfiguration} and include features such as treatment of filepatterns that don't
 * match anything and continuous incremental matching of filepatterns (watching for new files).
 *
 * <h3>Example: Watching a single filepattern for new files</h3>
 *
 * <p>This example matches a single filepattern repeatedly every 30 seconds, continuously returns
 * new matched files as an unbounded {@code PCollection<Metadata>} and stops if no new files appear
 * for 1 hour.
 *
 * <pre>{@code
 * PCollection<Metadata> matches = p.apply(FileIO.match()
 *     .filepattern("...")
 *     .continuously(
 *       Duration.standardSeconds(30), afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Example: Matching a PCollection of filepatterns arriving from Kafka</h3>
 *
 * <p>This example reads filepatterns from Kafka and matches each one as it arrives, producing again
 * an unbounded {@code PCollection<Metadata>}, and failing in case the filepattern doesn't match
 * anything.
 *
 * <pre>{@code
 * PCollection<String> filepatterns = p.apply(KafkaIO.read()...);
 *
 * PCollection<Metadata> matches = filepatterns.apply(FileIO.matchAll()
 *     .withEmptyMatchTreatment(DISALLOW));
 * }</pre>
 *
 * <h2>Reading files</h2>
 *
 * <p>{@link #readMatches} converts each result of {@link #match} or {@link #matchAll} to a {@link
 * ReadableFile} that is convenient for reading a file's contents, optionally decompressing it.
 *
 * <h3>Example: Returning filenames and contents of compressed files matching a filepattern</h3>
 *
 * <p>This example matches a single filepattern and returns {@code KVs} of filenames and their
 * contents as {@code String}, decompressing each file with GZIP.
 *
 * <pre>{@code
 * PCollection<KV<String, String>> filesAndContents = p
 *     .apply(FileIO.match().filepattern("hdfs://path/to/*.gz"))
 *     // withCompression can be omitted - by default compression is detected from the filename.
 *     .apply(FileIO.readMatches().withCompression(GZIP))
 *     .apply(MapElements
 *         // uses imports from TypeDescriptors
 *         .into(kvs(strings(), strings()))
 *         .via((ReadableFile f) -> {
 *           try {
 *             return KV.of(
 *                 f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
 *           } catch (IOException ex) {
 *             throw new RuntimeException("Failed to read the file", ex);
 *           }
 *         }));
 * }</pre>
 *
 * <h2>Writing files</h2>
 *
 * <p>{@link #write} and {@link #writeDynamic} write elements from a {@link PCollection} of a given
 * type to files, using a given {@link Sink} to write a set of elements to each file. The collection
 * can be bounded or unbounded - in either case, writing happens by default per window and pane, and
 * the amount of data in each window and pane is finite, so a finite number of files ("shards") are
 * written for each window and pane. There are several aspects to this process:
 *
 * <ul>
 *   <li><b>How many shards are generated per pane:</b> This is controlled by <i>sharding</i>, using
 *       {@link Write#withNumShards} or {@link Write#withSharding}. The default is runner-specific,
 *       so the number of shards will vary based on runner behavior, though at least 1 shard will
 *       always be produced for every non-empty pane. Note that setting a fixed number of shards can
 *       hurt performance: it adds an additional {@link GroupByKey} to the pipeline. However, it is
 *       required to set it when writing an unbounded {@link PCollection} due to <a
 *       href="https://issues.apache.org/jira/browse/BEAM-1438">BEAM-1438</a> and similar behavior
 *       in other runners.
 *   <li><b>How the shards are named:</b> This is controlled by a {@link Write.FileNaming}:
 *       filenames can depend on a variety of inputs, e.g. the window, the pane, total number of
 *       shards, the current file's shard index, and compression. Controlling the file naming is
 *       described in the section <i>File naming</i> below.
 *   <li><b>Which elements go into which shard:</b> Elements within a pane get distributed into
 *       different shards created for that pane arbitrarily, though {@link FileIO.Write} attempts to
 *       make shards approximately evenly sized. For more control over which elements go into which
 *       files, consider using <i>dynamic destinations</i> (see below).
 *   <li><b>How a given set of elements is written to a shard:</b> This is controlled by the {@link
 *       Sink}, e.g. {@link AvroIO#sink} will generate Avro files. The {@link Sink} controls the
 *       format of a single file: how to open a file, how to write each element to it, and how to
 *       close the file - but it does not control the set of files or which elements go where.
 *       Elements are written to a shard in an arbitrary order. {@link FileIO.Write} can
 *       additionally compress the generated files using {@link FileIO.Write#withCompression}.
 *   <li><b>How all of the above can be element-dependent:</b> This is controlled by <i>dynamic
 *       destinations</i>. It is possible to have different groups of elements use different
 *       policies for naming files and for configuring the {@link Sink}. See "dynamic destinations"
 *       below.
 * </ul>
 *
 * <h3>File naming</h3>
 *
 * <p>The names of generated files are produced by a {@link Write.FileNaming}. The default naming
 * strategy is to name files in the format: {@code
 * $prefix-$start-$end-$pane-$shard-of-$numShards$suffix$compressionSuffix}, where:
 *
 * <ul>
 *   <li>$prefix is set by {@link Write#withPrefix}, the default is "output".
 *   <li>$start and $end are boundaries of the window of data being written, formatted in ISO 8601
 *       format (YYYY-mm-ddTHH:MM:SSZZZ). The window is omitted in case this is the global window.
 *   <li>$pane is the index of the pane within the window. The pane is omitted in case it is known
 *       to be the only pane for this window.
 *   <li>$shard is the index of the current shard being written, out of the $numShards total shards
 *       written for the current pane. Both are formatted using 5 digits (or more if necessary
 *       according to $numShards) and zero-padded.
 *   <li>$suffix is set by {@link Write#withSuffix}, the default is empty.
 *   <li>$compressionSuffix is based on the default extension for the chosen {@link
 *       Write#withCompression compression type}.
 * </ul>
 *
 * <p>For example: {@code data-2017-12-01T19:00:00Z-2017-12-01T20:00:00Z-2-00010-of-00050.txt.gz}
 *
 * <p>Alternatively, one can specify a custom naming strategy using {@link
 * Write#withNaming(Write.FileNaming)}.
 *
 * <p>If {@link Write#to} is specified, then the filenames produced by the {@link Write.FileNaming}
 * are resolved relative to that directory.
 *
 * <p>When using dynamic destinations via {@link #writeDynamic} (see below), specifying a custom
 * naming strategy is required, using {@link Write#withNaming(SerializableFunction)} or {@link
 * Write#withNaming(Contextful)}. In those, pass a function that creates a {@link Write.FileNaming}
 * for the requested group ("destination"). You can either implement a custom {@link
 * Write.FileNaming}, or use {@link Write#defaultNaming} to configure the default naming strategy
 * with a prefix and suffix as per above.
 *
 * <h3>Dynamic destinations</h3>
 *
 * <p>If the elements in the input collection can be partitioned into groups that should be treated
 * differently, {@link FileIO.Write} supports different treatment per group ("destination"). It can
 * use different file naming strategies for different groups, and can differently configure the
 * {@link Sink}, e.g. write different elements to Avro files in different directories with different
 * schemas.
 *
 * <p>This feature is supported by {@link #writeDynamic}. Use {@link Write#by} to specify how too
 * partition the elements into groups ("destinations"). Then elements will be grouped by
 * destination, and {@link Write#withNaming(Contextful)} and {@link Write#via(Contextful)} will be
 * applied separately within each group, i.e. different groups will be written using the file naming
 * strategies returned by {@link Write#withNaming(Contextful)} and using sinks returned by {@link
 * Write#via(Contextful)} for the respective destinations. Note that currently sharding can not be
 * destination-dependent: every window/pane for every destination will use the same number of shards
 * specified via {@link Write#withNumShards} or {@link Write#withSharding}.
 *
 * <h3>Writing custom types to sinks</h3>
 *
 * <p>Normally, when writing a collection of a custom type using a {@link Sink} that takes a
 * different type (for example, writing a {@code PCollection<Event>} to a text-based {@code
 * Sink<String>}), one can simply apply a {@code ParDo} or {@code MapElements} to convert the custom
 * type to the sink's <i>output type</i>.
 *
 * <p>However, when using dynamic destinations, in many such cases the destination needs to be
 * extracted from the original type, so such a conversion is not possible. For example, one might
 * write events of a custom class {@code Event} to a text sink, using the event's "type" as a
 * destination. In that case, specify an <i>output function</i> in {@link Write#via(Contextful,
 * Contextful)} or {@link Write#via(Contextful, Sink)}.
 *
 * <h3>Example: Writing CSV files</h3>
 *
 * <pre>{@code
 * class CSVSink implements FileIO.Sink<List<String>> {
 *   private String header;
 *   private PrintWriter writer;
 *
 *   public CSVSink(List<String> colNames) {
 *     this.header = Joiner.on(",").join(colNames);
 *   }
 *
 *   public void open(WritableByteChannel channel) throws IOException {
 *     writer = new PrintWriter(Channels.newOutputStream(channel));
 *     writer.println(header);
 *   }
 *
 *   public void write(List<String> element) throws IOException {
 *     writer.println(Joiner.on(",").join(element));
 *   }
 *
 *   public void flush() throws IOException {
 *     writer.flush();
 *   }
 * }
 *
 * PCollection<BankTransaction> transactions = ...;
 * // Convert transactions to strings before writing them to the CSV sink.
 * transactions.apply(MapElements
 *         .into(TypeDescriptors.lists(TypeDescriptors.strings()))
 *         .via(tx -> Arrays.asList(tx.getUser(), tx.getAmount())))
 *     .apply(FileIO.<List<String>>write()
 *         .via(new CSVSink(Arrays.asList("user", "amount")))
 *         .to(".../path/to/")
 *         .withPrefix("transactions")
 *         .withSuffix(".csv"));
 * }</pre>
 *
 * <h3>Example: Writing CSV files to different directories and with different headers</h3>
 *
 * <pre>{@code
 * enum TransactionType {
 *   DEPOSIT,
 *   WITHDRAWAL,
 *   TRANSFER,
 *   ...
 *
 *   List<String> getFieldNames();
 *   List<String> getAllFields(BankTransaction tx);
 * }
 *
 * PCollection<BankTransaction> transactions = ...;
 * transactions.apply(FileIO.<TransactionType, Transaction>writeDynamic()
 *     .by(Transaction::getTypeName)
 *     .via(tx -> tx.getTypeName().toFields(tx),  // Convert the data to be written to CSVSink
 *          type -> new CSVSink(type.getFieldNames()))
 *     .to(".../path/to/")
 *     .withNaming(type -> defaultNaming(type + "-transactions", ".csv"));
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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
   *
   * <p>Returned {@link MatchResult.Metadata} are deduplicated by filename. For example, if this
   * transform observes a file with the same name several times with different metadata (e.g.
   * because the file is growing), it will emit the metadata the first time this file is observed,
   * and will ignore future changes to this file.
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

  /** Writes elements to files using a {@link Sink}. See class-level documentation. */
  public static <InputT> Write<Void, InputT> write() {
    return new AutoValue_FileIO_Write.Builder<Void, InputT>()
        .setDynamic(false)
        .setCompression(Compression.UNCOMPRESSED)
        .setIgnoreWindowing(false)
        .setNoSpilling(false)
        .build();
  }

  /**
   * Writes elements to files using a {@link Sink} and grouping the elements using "dynamic
   * destinations". See class-level documentation.
   */
  public static <DestT, InputT> Write<DestT, InputT> writeDynamic() {
    return new AutoValue_FileIO_Write.Builder<DestT, InputT>()
        .setDynamic(true)
        .setCompression(Compression.UNCOMPRESSED)
        .setIgnoreWindowing(false)
        .setNoSpilling(false)
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
      return (SeekableByteChannel) open();
    }

    /** Returns the full contents of the file as bytes. */
    public byte[] readFullyAsBytes() throws IOException {
      try (InputStream stream = Channels.newInputStream(open())) {
        return StreamUtils.getBytesWithoutClosing(stream);
      }
    }

    /** Returns the full contents of the file as a {@link String} decoded as UTF-8. */
    public String readFullyAsUTF8String() throws IOException {
      return new String(readFullyAsBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
      return "ReadableFile{metadata=" + metadata + ", compression=" + compression + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadableFile that = (ReadableFile) o;
      return Objects.equal(metadata, that.metadata) && compression == that.compression;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(metadata, compression);
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
          .setMatchUpdatedFiles(false)
          .build();
    }

    public abstract EmptyMatchTreatment getEmptyMatchTreatment();

    public abstract boolean getMatchUpdatedFiles();

    public abstract @Nullable Duration getWatchInterval();

    abstract @Nullable TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setMatchUpdatedFiles(boolean matchUpdatedFiles);

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
     *
     * <p>If {@code matchUpdatedFiles} is set, also watches for files with timestamp change, with
     * the watching frequency given by the {@code interval}. The pipeline will throw a {@code
     * RuntimeError} if timestamp extraction for the matched file has failed, suggesting the
     * timestamp metadata is not available with the IO connector.
     *
     * <p>Matching continuously scales poorly, as it is stateful, and requires storing file ids in
     * memory. In addition, because it is memory-only, if a pipeline is restarted, already processed
     * files will be reprocessed. Consider an alternate technique, such as <a
     * href="https://cloud.google.com/storage/docs/pubsub-notifications">Pub/Sub Notifications</a>
     * when using GCS if possible.
     */
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition, boolean matchUpdatedFiles) {
      LOG.warn(
          "Matching Continuously is stateful, and can scale poorly. Consider using Pub/Sub "
              + "Notifications (https://cloud.google.com/storage/docs/pubsub-notifications) if possible");
      return toBuilder()
          .setWatchInterval(interval)
          .setWatchTerminationCondition(condition)
          .setMatchUpdatedFiles(matchUpdatedFiles)
          .build();
    }

    /**
     * Continuously watches for new files at the given interval until the given termination
     * condition is reached, where the input to the condition is the filepattern. To watch also for
     * updated files, please set {@code matchUpdatedFiles} as {@code true}.
     */
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition) {
      return continuously(interval, condition, false);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("emptyMatchTreatment", getEmptyMatchTreatment().toString())
              .withLabel("Treatment of filepatterns that match no files"));
      if (getWatchInterval() != null) {
        builder
            .add(
                DisplayData.item("watchForNewFilesInterval", getWatchInterval())
                    .withLabel("Interval to watch for new files"))
            .add(
                DisplayData.item("isMatchUpdatedFiles", getMatchUpdatedFiles())
                    .withLabel("If also match for files with timestamp change"));
      }
    }
  }

  /** Implementation of {@link #match}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

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
      return this.filepattern(StaticValueProvider.of(filepattern));
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
     * <p>This works only in runners supporting splittable {@link
     * org.apache.beam.sdk.transforms.DoFn}.
     */
    public Match continuously(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withConfiguration(
          getConfiguration().continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    /**
     * See {@link MatchConfiguration#continuously}. The returned {@link PCollection} is unbounded.
     *
     * <p>This works only in runners supporting splittable {@link
     * org.apache.beam.sdk.transforms.DoFn}.
     */
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return continuously(pollInterval, terminationCondition, false);
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via MatchAll", matchAll().withConfiguration(getConfiguration()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .include("configuration", getConfiguration());
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

    /** Like {@link Match#continuously(Duration, TerminationCondition, boolean)}. */
    public MatchAll continuously(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withConfiguration(
          getConfiguration().continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    /** Like {@link Match#continuously(Duration, TerminationCondition)}. */
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return continuously(pollInterval, terminationCondition, false);
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input) {
      PCollection<MatchResult.Metadata> res;
      if (getConfiguration().getWatchInterval() == null) {
        res =
            input.apply(
                "Match filepatterns",
                ParDo.of(new MatchFn(getConfiguration().getEmptyMatchTreatment())));
      } else {
        if (getConfiguration().getMatchUpdatedFiles()) {
          res =
              input
                  .apply(createWatchTransform(new ExtractFilenameAndLastUpdateFn()))
                  .apply(Values.create())
                  .setCoder(MetadataCoderV2.of());
        } else {
          res = input.apply(createWatchTransform(new ExtractFilenameFn())).apply(Values.create());
        }
      }
      return res.apply(Reshuffle.viaRandomKey());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include("configuration", getConfiguration());
    }

    /** Helper function creating a watch transform based on outputKeyFn. */
    private <KeyT> Watch.Growth<String, MatchResult.Metadata, KeyT> createWatchTransform(
        SerializableFunction<MatchResult.Metadata, KeyT> outputKeyFn) {
      return Watch.growthOf(Contextful.of(new MatchPollFn(), Requirements.empty()), outputKeyFn)
          .withPollInterval(getConfiguration().getWatchInterval())
          .withTerminationPerInput(getConfiguration().getWatchTerminationCondition());
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

    private static class MatchPollFn extends PollFn<String, MatchResult.Metadata> {
      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String element, Context c)
          throws Exception {
        Instant now = Instant.now();
        return Watch.Growth.PollResult.incomplete(
                now, FileSystems.match(element, EmptyMatchTreatment.ALLOW).metadata())
            .withWatermark(now);
      }
    }

    private static class ExtractFilenameFn
        implements SerializableFunction<MatchResult.Metadata, String> {
      @Override
      public String apply(MatchResult.Metadata input) {
        return input.resourceId().toString();
      }
    }

    private static class ExtractFilenameAndLastUpdateFn
        implements SerializableFunction<MatchResult.Metadata, KV<String, Long>> {
      @Override
      public KV<String, Long> apply(MatchResult.Metadata input) throws RuntimeException {
        long timestamp = input.lastModifiedMillis();
        if (0L == timestamp) {
          throw new RuntimeException("Extract file timestamp failed: got file timestamp == 0.");
        }
        return KV.of(input.resourceId().toString(), timestamp);
      }
    }
  }

  /** Implementation of {@link #readMatches}. */
  @AutoValue
  public abstract static class ReadMatches
      extends PTransform<PCollection<MatchResult.Metadata>, PCollection<ReadableFile>> {
    /** Enum to control how directories are handled. */
    public enum DirectoryTreatment {
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

    /**
     * @return True if metadata is a directory and directory Treatment is SKIP.
     * @throws java.lang.IllegalArgumentException if metadata is a directory and directoryTreatment
     *     is Prohibited.
     * @throws java.lang.UnsupportedOperationException if metadata is a directory and
     *     directoryTreatment is not SKIP or PROHIBIT.
     */
    static boolean shouldSkipDirectory(
        MatchResult.Metadata metadata, DirectoryTreatment directoryTreatment) {
      if (metadata.resourceId().isDirectory()) {
        switch (directoryTreatment) {
          case SKIP:
            return true;
          case PROHIBIT:
            throw new IllegalArgumentException(
                "Trying to read " + metadata.resourceId() + " which is a directory");

          default:
            throw new UnsupportedOperationException(
                "Unknown DirectoryTreatment: " + directoryTreatment);
        }
      }

      return false;
    }

    /**
     * Converts metadata to readableFile. Make sure {@link
     * #shouldSkipDirectory(org.apache.beam.sdk.io.fs.MatchResult.Metadata,
     * org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment)} returns false before using.
     */
    static ReadableFile matchToReadableFile(
        MatchResult.Metadata metadata, Compression compression) {

      compression =
          (compression == Compression.AUTO)
              ? Compression.detect(metadata.resourceId().getFilename())
              : compression;
      return new ReadableFile(
          MatchResult.Metadata.builder()
              .setResourceId(metadata.resourceId())
              .setSizeBytes(metadata.sizeBytes())
              .setLastModifiedMillis(metadata.lastModifiedMillis())
              .setIsReadSeekEfficient(
                  metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
              .build(),
          compression);
    }

    private static class ToReadableFileFn extends DoFn<MatchResult.Metadata, ReadableFile> {
      private final ReadMatches spec;

      private ToReadableFileFn(ReadMatches spec) {
        this.spec = spec;
      }

      @ProcessElement
      public void process(ProcessContext c) {
        if (shouldSkipDirectory(c.element(), spec.getDirectoryTreatment())) {
          return;
        }
        ReadableFile r = matchToReadableFile(c.element(), spec.getCompression());
        c.output(r);
      }
    }
  }

  /**
   * Specifies how to write elements to individual files in {@link FileIO#write} and {@link
   * FileIO#writeDynamic}. A new instance of {@link Sink} is created for every file being written.
   */
  public interface Sink<ElementT> extends Serializable {
    /**
     * Initializes writing to the given channel. Will be invoked once on a given {@link Sink}
     * instance.
     */
    void open(WritableByteChannel channel) throws IOException;

    /** Appends a single element to the file. May be invoked zero or more times. */
    void write(ElementT element) throws IOException;

    /**
     * Flushes the buffered state (if any) before the channel is closed. Does not need to close the
     * channel. Will be invoked once.
     */
    void flush() throws IOException;
  }

  /** Implementation of {@link #write} and {@link #writeDynamic}. */
  @AutoValue
  public abstract static class Write<DestinationT, UserT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
    /** A policy for generating names for shard files. */
    public interface FileNaming extends Serializable {
      /**
       * Generates the filename. MUST use each argument and return different values for each
       * combination of the arguments.
       */
      String getFilename(
          BoundedWindow window,
          PaneInfo pane,
          int numShards,
          int shardIndex,
          Compression compression);
    }

    public static FileNaming defaultNaming(final String prefix, final String suffix) {
      return defaultNaming(StaticValueProvider.of(prefix), StaticValueProvider.of(suffix));
    }

    /**
     * Defines a default {@link FileNaming} which will use the prefix and suffix supplied to create
     * a name based on the window, pane, number of shards, shard index, and compression. Removes
     * window when in the {@link GlobalWindow} and pane info when it is the only firing of the pane.
     */
    public static FileNaming defaultNaming(
        final ValueProvider<String> prefix, final ValueProvider<String> suffix) {
      return (window, pane, numShards, shardIndex, compression) -> {
        checkArgument(window != null, "window can not be null");
        checkArgument(pane != null, "pane can not be null");
        checkArgument(compression != null, "compression can not be null");
        StringBuilder res = new StringBuilder(prefix.get());
        if (window != GlobalWindow.INSTANCE) {
          if (res.length() > 0) {
            res.append("-");
          }
          checkArgument(
              window instanceof IntervalWindow,
              "defaultNaming() supports only windows of type %s, " + "but got window %s of type %s",
              IntervalWindow.class.getSimpleName(),
              window,
              window.getClass().getSimpleName());
          IntervalWindow iw = (IntervalWindow) window;
          res.append(iw.start().toString()).append("-").append(iw.end().toString());
        }
        boolean isOnlyFiring = pane.isFirst() && pane.isLast();
        if (!isOnlyFiring) {
          if (res.length() > 0) {
            res.append("-");
          }
          res.append(pane.getIndex());
        }
        if (res.length() > 0) {
          res.append("-");
        }
        String numShardsStr = String.valueOf(numShards);
        // A trillion shards per window per pane ought to be enough for everybody.
        DecimalFormat df =
            new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
        res.append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
        res.append(suffix.get());
        res.append(compression.getSuggestedSuffix());
        return res.toString();
      };
    }

    public static FileNaming relativeFileNaming(
        final ValueProvider<String> baseDirectory, final FileNaming innerNaming) {
      return (window, pane, numShards, shardIndex, compression) ->
          FileSystems.matchNewResource(baseDirectory.get(), true /* isDirectory */)
              .resolve(
                  innerNaming.getFilename(window, pane, numShards, shardIndex, compression),
                  RESOLVE_FILE)
              .toString();
    }

    abstract boolean getDynamic();

    abstract @Nullable Contextful<Fn<DestinationT, Sink<?>>> getSinkFn();

    abstract @Nullable Contextful<Fn<UserT, ?>> getOutputFn();

    abstract @Nullable Contextful<Fn<UserT, DestinationT>> getDestinationFn();

    abstract @Nullable ValueProvider<String> getOutputDirectory();

    abstract @Nullable ValueProvider<String> getFilenamePrefix();

    abstract @Nullable ValueProvider<String> getFilenameSuffix();

    abstract @Nullable FileNaming getConstantFileNaming();

    abstract @Nullable Contextful<Fn<DestinationT, FileNaming>> getFileNamingFn();

    abstract @Nullable DestinationT getEmptyWindowDestination();

    abstract @Nullable Coder<DestinationT> getDestinationCoder();

    abstract @Nullable ValueProvider<String> getTempDirectory();

    abstract Compression getCompression();

    abstract @Nullable ValueProvider<Integer> getNumShards();

    abstract @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding();

    abstract boolean getIgnoreWindowing();

    abstract boolean getNoSpilling();

    abstract Builder<DestinationT, UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<DestinationT, UserT> {
      abstract Builder<DestinationT, UserT> setDynamic(boolean dynamic);

      abstract Builder<DestinationT, UserT> setSinkFn(Contextful<Fn<DestinationT, Sink<?>>> sink);

      abstract Builder<DestinationT, UserT> setOutputFn(Contextful<Fn<UserT, ?>> outputFn);

      abstract Builder<DestinationT, UserT> setDestinationFn(
          Contextful<Fn<UserT, DestinationT>> destinationFn);

      abstract Builder<DestinationT, UserT> setOutputDirectory(
          ValueProvider<String> outputDirectory);

      abstract Builder<DestinationT, UserT> setFilenamePrefix(ValueProvider<String> filenamePrefix);

      abstract Builder<DestinationT, UserT> setFilenameSuffix(ValueProvider<String> filenameSuffix);

      abstract Builder<DestinationT, UserT> setConstantFileNaming(FileNaming constantFileNaming);

      abstract Builder<DestinationT, UserT> setFileNamingFn(
          Contextful<Fn<DestinationT, FileNaming>> namingFn);

      abstract Builder<DestinationT, UserT> setEmptyWindowDestination(
          DestinationT emptyWindowDestination);

      abstract Builder<DestinationT, UserT> setDestinationCoder(
          Coder<DestinationT> destinationCoder);

      abstract Builder<DestinationT, UserT> setTempDirectory(
          ValueProvider<String> tempDirectoryProvider);

      abstract Builder<DestinationT, UserT> setCompression(Compression compression);

      abstract Builder<DestinationT, UserT> setNumShards(
          @Nullable ValueProvider<Integer> numShards);

      abstract Builder<DestinationT, UserT> setSharding(
          PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding);

      abstract Builder<DestinationT, UserT> setIgnoreWindowing(boolean ignoreWindowing);

      abstract Builder<DestinationT, UserT> setNoSpilling(boolean noSpilling);

      abstract Write<DestinationT, UserT> build();
    }

    /** Specifies how to partition elements into groups ("destinations"). */
    public Write<DestinationT, UserT> by(SerializableFunction<UserT, DestinationT> destinationFn) {
      checkArgument(destinationFn != null, "destinationFn can not be null");
      return by(fn(destinationFn));
    }

    /** Like {@link #by}, but with access to context such as side inputs. */
    public Write<DestinationT, UserT> by(Contextful<Fn<UserT, DestinationT>> destinationFn) {
      checkArgument(destinationFn != null, "destinationFn can not be null");
      return toBuilder().setDestinationFn(destinationFn).build();
    }

    /**
     * Specifies how to create a {@link Sink} for a particular destination and how to map the
     * element type to the sink's output type. The sink function must create a new {@link Sink}
     * instance every time it is called.
     */
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn,
        Contextful<Fn<DestinationT, Sink<OutputT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return toBuilder().setSinkFn((Contextful) sinkFn).setOutputFn(outputFn).build();
    }

    /** Like {@link #via(Contextful, Contextful)}, but uses the same sink for all destinations. */
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn, final Sink<OutputT> sink) {
      checkArgument(sink != null, "sink can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return via(outputFn, fn(SerializableFunctions.clonesOf(sink)));
    }

    /**
     * Like {@link #via(Contextful, Contextful)}, but the output type of the sink is the same as the
     * type of the input collection. The sink function must create a new {@link Sink} instance every
     * time it is called.
     */
    public Write<DestinationT, UserT> via(Contextful<Fn<DestinationT, Sink<UserT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      return toBuilder()
          .setSinkFn((Contextful) sinkFn)
          .setOutputFn(fn(SerializableFunctions.<UserT>identity()))
          .build();
    }

    /** Like {@link #via(Contextful)}, but uses the same {@link Sink} for all destinations. */
    public Write<DestinationT, UserT> via(Sink<UserT> sink) {
      checkArgument(sink != null, "sink can not be null");
      return via(fn(SerializableFunctions.clonesOf(sink)));
    }

    /**
     * Specifies a common directory for all generated files. A temporary generated sub-directory of
     * this directory will be used as the temp directory, unless overridden by {@link
     * #withTempDirectory}.
     */
    public Write<DestinationT, UserT> to(String directory) {
      checkArgument(directory != null, "directory can not be null");
      return to(StaticValueProvider.of(directory));
    }

    /** Like {@link #to(String)} but with a {@link ValueProvider}. */
    public Write<DestinationT, UserT> to(ValueProvider<String> directory) {
      checkArgument(directory != null, "directory can not be null");
      return toBuilder().setOutputDirectory(directory).build();
    }

    /**
     * Specifies a common prefix to use for all generated filenames, if using the default file
     * naming. Incompatible with {@link #withNaming}.
     */
    public Write<DestinationT, UserT> withPrefix(String prefix) {
      checkArgument(prefix != null, "prefix can not be null");
      return withPrefix(StaticValueProvider.of(prefix));
    }

    /** Like {@link #withPrefix(String)} but with a {@link ValueProvider}. */
    public Write<DestinationT, UserT> withPrefix(ValueProvider<String> prefix) {
      checkArgument(prefix != null, "prefix can not be null");
      return toBuilder().setFilenamePrefix(prefix).build();
    }

    /**
     * Specifies a common suffix to use for all generated filenames, if using the default file
     * naming. Incompatible with {@link #withNaming}.
     */
    public Write<DestinationT, UserT> withSuffix(String suffix) {
      checkArgument(suffix != null, "suffix can not be null");
      return withSuffix(StaticValueProvider.of(suffix));
    }

    /** Like {@link #withSuffix(String)} but with a {@link ValueProvider}. */
    public Write<DestinationT, UserT> withSuffix(ValueProvider<String> suffix) {
      checkArgument(suffix != null, "suffix can not be null");
      return toBuilder().setFilenameSuffix(suffix).build();
    }

    /**
     * Specifies a custom strategy for generating filenames. All generated filenames will be
     * resolved relative to the directory specified in {@link #to}, if any.
     *
     * <p>Incompatible with {@link #withSuffix}.
     *
     * <p>This can only be used in combination with {@link #write()} but not {@link
     * #writeDynamic()}.
     */
    public Write<DestinationT, UserT> withNaming(FileNaming naming) {
      checkArgument(naming != null, "naming can not be null");
      return toBuilder().setConstantFileNaming(naming).build();
    }

    /**
     * Specifies a custom strategy for generating filenames depending on the destination, similar to
     * {@link #withNaming(FileNaming)}.
     *
     * <p>This can only be used in combination with {@link #writeDynamic()} but not {@link
     * #write()}.
     */
    public Write<DestinationT, UserT> withNaming(
        SerializableFunction<DestinationT, FileNaming> namingFn) {
      checkArgument(namingFn != null, "namingFn can not be null");
      return withNaming(fn(namingFn));
    }

    /**
     * Like {@link #withNaming(SerializableFunction)} but allows accessing context, such as side
     * inputs, from the function.
     */
    public Write<DestinationT, UserT> withNaming(
        Contextful<Fn<DestinationT, FileNaming>> namingFn) {
      checkArgument(namingFn != null, "namingFn can not be null");
      return toBuilder().setFileNamingFn(namingFn).build();
    }

    /** Specifies a directory into which all temporary files will be placed. */
    public Write<DestinationT, UserT> withTempDirectory(String tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    /** Like {@link #withTempDirectory(String)}. */
    public Write<DestinationT, UserT> withTempDirectory(ValueProvider<String> tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    /**
     * Specifies to compress all generated shard files using the given {@link Compression} and, by
     * default, append the respective extension to the filename.
     */
    public Write<DestinationT, UserT> withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      checkArgument(
          compression != Compression.AUTO, "AUTO compression is not supported for writing");
      return toBuilder().setCompression(compression).build();
    }

    /**
     * If {@link #withIgnoreWindowing()} is specified, specifies a destination to be used in case
     * the collection is empty, to generate the (only, empty) output file.
     */
    public Write<DestinationT, UserT> withEmptyGlobalWindowDestination(
        DestinationT emptyWindowDestination) {
      return toBuilder().setEmptyWindowDestination(emptyWindowDestination).build();
    }

    /**
     * Specifies a {@link Coder} for the destination type, if it can not be inferred from {@link
     * #by}.
     */
    public Write<DestinationT, UserT> withDestinationCoder(Coder<DestinationT> destinationCoder) {
      checkArgument(destinationCoder != null, "destinationCoder can not be null");
      return toBuilder().setDestinationCoder(destinationCoder).build();
    }

    /**
     * Specifies to use a given fixed number of shards per window. 0 means runner-determined
     * sharding. Specifying a non-zero value may hurt performance, because it will limit the
     * parallelism of writing and will introduce an extra {@link GroupByKey} operation.
     */
    public Write<DestinationT, UserT> withNumShards(int numShards) {
      checkArgument(numShards >= 0, "numShards must be non-negative, but was: %s", numShards);
      if (numShards == 0) {
        return withNumShards(null);
      }
      return withNumShards(StaticValueProvider.of(numShards));
    }

    /**
     * Like {@link #withNumShards(int)}. Specifying {@code null} means runner-determined sharding.
     */
    public Write<DestinationT, UserT> withNumShards(@Nullable ValueProvider<Integer> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Specifies a {@link PTransform} to use for computing the desired number of shards in each
     * window.
     */
    public Write<DestinationT, UserT> withSharding(
        PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding) {
      checkArgument(sharding != null, "sharding can not be null");
      return toBuilder().setSharding(sharding).build();
    }

    /**
     * Specifies to ignore windowing information in the input, and instead rewindow it to global
     * window with the default trigger.
     *
     * @deprecated Avoid usage of this method: its effects are complex and it will be removed in
     *     future versions of Beam. Right now it exists for compatibility with {@link WriteFiles}.
     */
    @Deprecated
    public Write<DestinationT, UserT> withIgnoreWindowing() {
      return toBuilder().setIgnoreWindowing(true).build();
    }

    /** See {@link WriteFiles#withNoSpilling()}. */
    public Write<DestinationT, UserT> withNoSpilling() {
      return toBuilder().setNoSpilling(true).build();
    }

    @VisibleForTesting
    Contextful<Fn<DestinationT, FileNaming>> resolveFileNamingFn() {
      if (getDynamic()) {
        checkArgument(
            getConstantFileNaming() == null,
            "when using writeDynamic(), must use versions of .withNaming() "
                + "that take functions from DestinationT");
        checkArgument(getFilenamePrefix() == null, ".withPrefix() requires write()");
        checkArgument(getFilenameSuffix() == null, ".withSuffix() requires write()");
        checkArgument(
            getFileNamingFn() != null,
            "when using writeDynamic(), must specify "
                + ".withNaming() taking a function form DestinationT");
        return fn(
            (element, c) -> {
              FileNaming naming = getFileNamingFn().getClosure().apply(element, c);
              return getOutputDirectory() == null
                  ? naming
                  : relativeFileNaming(getOutputDirectory(), naming);
            },
            getFileNamingFn().getRequirements());
      } else {
        checkArgument(
            getFileNamingFn() == null,
            ".withNaming() taking a function from DestinationT requires writeDynamic()");
        FileNaming constantFileNaming;
        if (getConstantFileNaming() == null) {
          constantFileNaming =
              defaultNaming(
                  MoreObjects.firstNonNull(getFilenamePrefix(), StaticValueProvider.of("output")),
                  MoreObjects.firstNonNull(getFilenameSuffix(), StaticValueProvider.of("")));
        } else {
          checkArgument(
              getFilenamePrefix() == null, ".to(FileNaming) is incompatible with .withSuffix()");
          checkArgument(
              getFilenameSuffix() == null, ".to(FileNaming) is incompatible with .withPrefix()");
          constantFileNaming = getConstantFileNaming();
        }
        if (getOutputDirectory() != null) {
          constantFileNaming = relativeFileNaming(getOutputDirectory(), constantFileNaming);
        }
        return fn(SerializableFunctions.<DestinationT, FileNaming>constant(constantFileNaming));
      }
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      Write.Builder<DestinationT, UserT> resolvedSpec = new AutoValue_FileIO_Write.Builder<>();

      resolvedSpec.setDynamic(getDynamic());

      checkArgument(getSinkFn() != null, ".via() is required");
      resolvedSpec.setSinkFn(getSinkFn());

      checkArgument(getOutputFn() != null, "outputFn should have been set by .via()");
      resolvedSpec.setOutputFn(getOutputFn());

      // Resolve destinationFn
      if (getDynamic()) {
        checkArgument(getDestinationFn() != null, "when using writeDynamic(), .by() is required");
        resolvedSpec.setDestinationFn(getDestinationFn());
        resolvedSpec.setDestinationCoder(resolveDestinationCoder(input));
      } else {
        checkArgument(getDestinationFn() == null, ".by() requires writeDynamic()");
        checkArgument(
            getDestinationCoder() == null, ".withDestinationCoder() requires writeDynamic()");
        resolvedSpec.setDestinationFn(fn(SerializableFunctions.constant(null)));
        resolvedSpec.setDestinationCoder((Coder) VoidCoder.of());
      }

      resolvedSpec.setFileNamingFn(resolveFileNamingFn());
      resolvedSpec.setEmptyWindowDestination(getEmptyWindowDestination());
      if (getTempDirectory() == null) {
        checkArgument(
            getOutputDirectory() != null, "must specify either .withTempDirectory() or .to()");
        resolvedSpec.setTempDirectory(getOutputDirectory());
      } else {
        resolvedSpec.setTempDirectory(getTempDirectory());
      }

      resolvedSpec.setCompression(getCompression());
      resolvedSpec.setNumShards(getNumShards());
      resolvedSpec.setSharding(getSharding());
      resolvedSpec.setIgnoreWindowing(getIgnoreWindowing());
      resolvedSpec.setNoSpilling(getNoSpilling());

      Write<DestinationT, UserT> resolved = resolvedSpec.build();
      WriteFiles<UserT, DestinationT, ?> writeFiles =
          WriteFiles.to(new ViaFileBasedSink<>(resolved))
              .withSideInputs(Lists.newArrayList(resolved.getAllSideInputs()));
      if (getNumShards() != null) {
        writeFiles = writeFiles.withNumShards(getNumShards());
      } else if (getSharding() != null) {
        writeFiles = writeFiles.withSharding(getSharding());
      } else {
        writeFiles = writeFiles.withRunnerDeterminedSharding();
      }
      if (!getIgnoreWindowing()) {
        writeFiles = writeFiles.withWindowedWrites();
      }
      if (getNoSpilling()) {
        writeFiles = writeFiles.withNoSpilling();
      }
      return input.apply(writeFiles);
    }

    private Coder<DestinationT> resolveDestinationCoder(PCollection<UserT> input) {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder == null) {
        TypeDescriptor<DestinationT> destinationT =
            TypeDescriptors.outputOf(getDestinationFn().getClosure());
        try {
          destinationCoder = input.getPipeline().getCoderRegistry().getCoder(destinationT);
        } catch (CannotProvideCoderException e) {
          throw new IllegalArgumentException(
              "Unable to infer a coder for destination type (inferred from .by() as \""
                  + destinationT
                  + "\") - specify it explicitly using .withDestinationCoder()");
        }
      }
      return destinationCoder;
    }

    private Collection<PCollectionView<?>> getAllSideInputs() {
      return Requirements.union(getDestinationFn(), getOutputFn(), getSinkFn(), getFileNamingFn())
          .getSideInputs();
    }

    private static class ViaFileBasedSink<UserT, DestinationT, OutputT>
        extends FileBasedSink<UserT, DestinationT, OutputT> {
      private final Write<DestinationT, UserT> spec;

      private ViaFileBasedSink(Write<DestinationT, UserT> spec) {
        super(
            ValueProvider.NestedValueProvider.of(
                spec.getTempDirectory(),
                input -> FileSystems.matchNewResource(input, true /* isDirectory */)),
            new DynamicDestinationsAdapter<>(spec),
            spec.getCompression());
        this.spec = spec;
      }

      @Override
      public WriteOperation<DestinationT, OutputT> createWriteOperation() {
        return new WriteOperation<DestinationT, OutputT>(this) {
          @Override
          public Writer<DestinationT, OutputT> createWriter() throws Exception {
            return new Writer<DestinationT, OutputT>(this, "") {
              private @Nullable Sink<OutputT> sink;

              @Override
              protected void prepareWrite(WritableByteChannel channel) throws Exception {
                Fn<DestinationT, Sink<OutputT>> sinkFn = (Fn) spec.getSinkFn().getClosure();
                sink =
                    sinkFn.apply(
                        getDestination(),
                        new Fn.Context() {
                          @Override
                          public <T> T sideInput(PCollectionView<T> view) {
                            return getWriteOperation()
                                .getSink()
                                .getDynamicDestinations()
                                .sideInput(view);
                          }
                        });
                sink.open(channel);
              }

              @Override
              public void write(OutputT value) throws Exception {
                sink.write(value);
              }

              @Override
              protected void finishWrite() throws Exception {
                sink.flush();
              }
            };
          }
        };
      }

      private static class DynamicDestinationsAdapter<UserT, DestinationT, OutputT>
          extends DynamicDestinations<UserT, DestinationT, OutputT> {
        private final Write<DestinationT, UserT> spec;
        private transient Fn.@Nullable Context context;

        private DynamicDestinationsAdapter(Write<DestinationT, UserT> spec) {
          this.spec = spec;
        }

        private Fn.Context getContext() {
          if (context == null) {
            context =
                new Fn.Context() {
                  @Override
                  public <T> T sideInput(PCollectionView<T> view) {
                    return DynamicDestinationsAdapter.this.sideInput(view);
                  }
                };
          }
          return context;
        }

        @Override
        public OutputT formatRecord(UserT record) {
          try {
            return ((Fn<UserT, OutputT>) spec.getOutputFn().getClosure())
                .apply(record, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDestination(UserT element) {
          try {
            return spec.getDestinationFn().getClosure().apply(element, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDefaultDestination() {
          return spec.getEmptyWindowDestination();
        }

        @Override
        public FilenamePolicy getFilenamePolicy(final DestinationT destination) {
          final FileNaming namingFn;
          try {
            namingFn = spec.getFileNamingFn().getClosure().apply(destination, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return new FilenamePolicy() {
            @Override
            public ResourceId windowedFilename(
                int shardNumber,
                int numShards,
                BoundedWindow window,
                PaneInfo paneInfo,
                OutputFileHints outputFileHints) {
              // We ignore outputFileHints because it will always be the same as
              // spec.getCompression() because we control the FileBasedSink.
              return FileSystems.matchNewResource(
                  namingFn.getFilename(
                      window, paneInfo, numShards, shardNumber, spec.getCompression()),
                  false /* isDirectory */);
            }

            @Override
            public @Nullable ResourceId unwindowedFilename(
                int shardNumber, int numShards, OutputFileHints outputFileHints) {
              return FileSystems.matchNewResource(
                  namingFn.getFilename(
                      GlobalWindow.INSTANCE,
                      PaneInfo.NO_FIRING,
                      numShards,
                      shardNumber,
                      spec.getCompression()),
                  false /* isDirectory */);
            }
          };
        }

        @Override
        public List<PCollectionView<?>> getSideInputs() {
          return Lists.newArrayList(spec.getAllSideInputs());
        }

        @Override
        public @Nullable Coder<DestinationT> getDestinationCoder() {
          return spec.getDestinationCoder();
        }
      }
    }
  }
}
