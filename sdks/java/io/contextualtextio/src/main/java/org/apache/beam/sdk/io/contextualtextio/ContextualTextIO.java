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
package org.apache.beam.sdk.io.contextualtextio;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.contextualtextio.ContextualTextIO.Read.AddFileNameAndRange;
import org.apache.beam.sdk.io.contextualtextio.ContextualTextIO.Read.AssignRecordNums;
import org.apache.beam.sdk.io.contextualtextio.ContextualTextIO.Read.ComputeRecordsBeforeEachRange;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s that read text files and collect contextual information of the elements in
 * the input.
 *
 * <p>Prefer {@link TextIO} when not reading files with multi-line records or additional record
 * metadata is not required.
 *
 * <h2>Reading from text files</h2>
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code
 * ContextualTextIO.read()}. To instantiate a transform use {@link
 * ContextualTextIO.Read#from(String)} and specify the path of the file(s) to be read.
 * Alternatively, if the filenames to be read are themselves in a {@link PCollection} you can use
 * {@link FileIO} to match them and {@link ContextualTextIO#readFiles()} to read them.
 *
 * <p>{@link #read} returns a {@link PCollection} of {@link Row}s with schema {@link
 * RecordWithMetadata#getSchema()}, each corresponding to one line of an input UTF-8 text file
 * (split into lines delimited by '\n', '\r', '\r\n', or specified delimiter via {@link
 * ContextualTextIO.Read#withDelimiter}).
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, the filepatterns are expanded only once. The combination of {@link
 * FileIO.Match#continuously(Duration, TerminationCondition)} and {@link #readFiles()} allow
 * streaming of new files matching the filepattern(s).
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readFiles()}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * ContextualTextIO.Read#withEmptyMatchTreatment} or {@link
 * FileIO.Match#withEmptyMatchTreatment(EmptyMatchTreatment)} plus {@link #readFiles()} to configure
 * this behavior.
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a file:
 * PCollection<Row> records = p.apply(ContextualTextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>Example 2: reading a PCollection of filenames.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<Row> records =
 *     filenames
 *         .apply(FileIO.matchAll())
 *         .apply(FileIO.readMatches())
 *         .apply(ContextualTextIO.readFiles());
 * }</pre>
 *
 * <p>Example 3: streaming new files matching a filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Row> records = p.apply(ContextualTextIO.read()
 *     .from("/local/path/to/files/*")
 *     .watchForNewFiles(
 *       // Check for new files every minute
 *       Duration.standardMinutes(1),
 *       // Stop watching the filepattern if no new files appear within an hour
 *       afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <p>Example 4: reading a file or file pattern of RFC4180-compliant CSV files with fields that may
 * contain line breaks.
 *
 * <p>Example of such a file could be:
 *
 * <p>"aaa","b CRLF bb","ccc" CRLF zzz,yyy,xxx
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Row> records = p.apply(ContextualTextIO.read()
 *     .from("/local/path/to/files/*.csv")
 *      .withHasMultilineCSVRecords(true));
 * }</pre>
 *
 * <p>Example 5: reading while watching for new files
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Row> records = p.apply(FileIO.match()
 *      .filepattern("filepattern")
 *      .continuously(
 *        Duration.millis(100),
 *        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))))
 *      .apply(FileIO.readMatches())
 *      .apply(ContextualTextIO.readFiles());
 * }</pre>
 *
 * <p>Example 6: reading with recordNum metadata.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<Row> records = p.apply(ContextualTextIO.read()
 *     .from("/local/path/to/files/*.csv")
 *      .setWithRecordNumMetadata(true));
 * }</pre>
 *
 * <p>NOTE: When using {@link ContextualTextIO.Read#withHasMultilineCSVRecords(Boolean)}, a single
 * reader will be used to process the file, rather than multiple readers which can read from
 * different offsets. For a large file this can result in lower performance.
 *
 * <p>NOTE: Use {@link Read#withRecordNumMetadata()} when recordNum metadata is required. Computing
 * absolute record positions currently introduces a grouping step, which increases the resources
 * used by the pipeline. By default withRecordNumMetadata is set to false, in this case record
 * objects will not contain absolute record positions within the entire file, but will still contain
 * relative positions in respective offsets.
 *
 * <h3>Reading a very large number of files</h3>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link ContextualTextIO.Read#withHintMatchesManyFiles} for better
 * performance and scalability. Note that it may decrease performance if the filepattern matches
 * only a small number of files.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ContextualTextIO {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;
  private static final Logger LOG = LoggerFactory.getLogger(ContextualTextIO.class);

  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded {@link
   * PCollection} containing one {@link Row element} for each line in the input files.
   */
  public static Read read() {
    return new AutoValue_ContextualTextIO_Read.Builder()
        .setCompression(Compression.AUTO)
        .setHintMatchesManyFiles(false)
        .setWithRecordNumMetadata(false)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .setHasMultilineCSVRecords(false)
        .build();
  }

  /**
   * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
   * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
   */
  public static ReadFiles readFiles() {
    return new AutoValue_ContextualTextIO_ReadFiles.Builder()
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .setHasMultilineCSVRecords(false)
        .setWithRecordNumMetadata(false)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {
    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract boolean getHintMatchesManyFiles();

    abstract boolean getWithRecordNumMetadata();

    abstract Compression getCompression();

    abstract @Nullable Boolean getHasMultilineCSVRecords();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    abstract byte @Nullable [] getDelimiter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Builder setWithRecordNumMetadata(boolean withSortedLineNumMetadata);

      abstract Builder setCompression(Compression compression);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

      abstract Builder setHasMultilineCSVRecords(Boolean hasMultilineCSVRecords);

      abstract Read build();
    }

    /**
     * Reads text from the file(s) with the given filename or filename pattern.
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
      checkArgument(filepattern != null, "filepattern can not be null");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkArgument(filepattern != null, "filepattern can not be null");
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    /**
     * When reading RFC4180 CSV files that have values that span multiple lines, set this to true.
     * Note: this reduces the read performance (see: {@link ContextualTextIO}).
     */
    public Read withHasMultilineCSVRecords(Boolean hasMultilineCSVRecords) {
      return toBuilder().setHasMultilineCSVRecords(hasMultilineCSVRecords).build();
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

    /**
     * Allows the user to opt into getting recordNums associated with each record. <b> This option
     * is only supported with default triggers.</b>
     *
     * <p>When set to true, it will introduce a grouping step to assemble the recordNums for each
     * record, which will increase the resources used by the pipeline.
     *
     * <p>Use this when you need metadata like fileNames and you need processed position/order
     * information.
     */
    public Read withRecordNumMetadata() {
      return toBuilder().setWithRecordNumMetadata(true).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment}. */
    public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n'). */
    public Read withDelimiter(byte[] delimiter) {
      checkArgument(delimiter != null, "delimiter can not be null");
      checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
      return toBuilder().setDelimiter(delimiter).build();
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
    public PCollection<Row> expand(PBegin input) {
      checkNotNull(
          getFilepattern(), "need to set the filepattern of a ContextualTextIO.Read transform");
      PCollection<Row> records = null;
      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        records = input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
      } else {
        // All other cases go through FileIO + ReadFiles
        records =
            input
                .apply(
                    "Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                .apply(
                    "Read Matches",
                    FileIO.readMatches()
                        .withCompression(getCompression())
                        .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                .apply("Via ReadFiles", readFiles().withDelimiter(getDelimiter()));
      }

      // Check if the user decided to opt out of recordNums associated with records
      if (!getWithRecordNumMetadata()) {
        return records;
      }

      return records.apply(new ProcessRecordNumbers());
    }

    @VisibleForTesting
    static class AddFileNameAndRange extends DoFn<Row, KV<KV<String, Long>, Row>> {
      @ProcessElement
      public void processElement(
          @Element Row record, OutputReceiver<KV<KV<String, Long>, Row>> out) {

        out.output(
            KV.of(
                KV.of(
                    record
                        .getLogicalTypeValue(RecordWithMetadata.RESOURCE_ID, ResourceId.class)
                        .toString(),
                    record.getInt64(RecordWithMetadata.RANGE_OFFSET)),
                record));
      }
    }

    /**
     * Helper class for computing number of record in the File preceding the beginning of the Range
     * in this file.
     */
    @VisibleForTesting
    static class ComputeRecordsBeforeEachRange extends DoFn<Integer, KV<String, KV<Long, Long>>> {
      private final PCollectionView<Map<String, Iterable<KV<Long, Long>>>> rangeSizes;

      public ComputeRecordsBeforeEachRange(
          PCollectionView<Map<String, Iterable<KV<Long, Long>>>> rangeSizes) {
        this.rangeSizes = rangeSizes;
      }

      // Add custom comparator as KV<K, V> is not comparable by default
      private static class FileRangeComparator<K extends Comparable<K>, V extends Comparable<V>>
          implements Comparator<KV<K, V>>, Serializable {
        @Override
        public int compare(KV<K, V> a, KV<K, V> b) {
          if (a.getKey().compareTo(b.getKey()) == 0) {
            return a.getValue().compareTo(b.getValue());
          }
          return a.getKey().compareTo(b.getKey());
        }
      }

      @ProcessElement
      public void processElement(ProcessContext p) {
        // Get the multimap side input containing the size of each read range.
        Map<String, Iterable<KV<Long, Long>>> rangeSizesMap = p.sideInput(rangeSizes);

        // Process each file, retrieving each filename as key from the side input.
        for (Entry<String, Iterable<KV<Long, Long>>> entrySet : rangeSizesMap.entrySet()) {
          // The FileRange Pair must be sorted.
          // TODO: We don't need to attach the filename during sorting since we process all
          // ranges within the same file.
          SortedMap<KV<String, Long>, Long> sorted = new TreeMap<>(new FileRangeComparator<>());

          entrySet
              .getValue()
              .iterator()
              .forEachRemaining(
                  x -> sorted.put(KV.of(entrySet.getKey(), x.getKey()), x.getValue()));

          // HashMap that tracks number of records passed for each file
          Map<String, Long> pastRecords = new HashMap<>();

          // For each (File, Range) Pair, compute the number of records before it
          for (Map.Entry<KV<String, Long>, Long> entry : sorted.entrySet()) {
            Long numRecords = entry.getValue();
            KV<String, Long> fileRange = entry.getKey();
            String file = fileRange.getKey();
            Long numRecordsBefore = 0L;
            if (pastRecords.containsKey(file)) {
              numRecordsBefore = pastRecords.get(file);
            }
            p.output(KV.of(file, KV.of(fileRange.getValue(), numRecordsBefore)));
            pastRecords.put(file, numRecordsBefore + numRecords);
          }
        }
      }
    }

    /**
     * Helper transform for computing absolute position of each record given the read range of each
     * record, relative position within range, and a side input describing the number of records
     * that precede the beginning of each read range.
     */
    static class AssignRecordNums extends DoFn<KV<KV<String, Long>, Row>, Row> {
      PCollectionView<Map<String, Iterable<KV<Long, Long>>>> numRecordsBeforeEachRange;

      public AssignRecordNums(
          PCollectionView<Map<String, Iterable<KV<Long, Long>>>> numRecordsBeforeEachRange) {
        this.numRecordsBeforeEachRange = numRecordsBeforeEachRange;
      }

      @ProcessElement
      public void processElement(ProcessContext p) {
        String file = p.element().getKey().getKey();
        Long offset = p.element().getKey().getValue();
        Row record = p.element().getValue();

        Iterator<KV<Long, Long>> numRecordsBeforeEachOffsetInFile =
            p.sideInput(numRecordsBeforeEachRange).get(file).iterator();
        Long numRecordsLessThanThisOffset =
            getNumRecordsBeforeOffset(offset, numRecordsBeforeEachOffsetInFile);

        Row newLine =
            Row.fromRow(record)
                .withFieldValue(
                    RecordWithMetadata.RECORD_NUM,
                    record.getInt64(RecordWithMetadata.RECORD_NUM_IN_OFFSET)
                        + numRecordsLessThanThisOffset)
                .build();
        p.output(newLine);
      }

      private Long getNumRecordsBeforeOffset(
          Long offset, Iterator<KV<Long, Long>> numRecordsBeforeEachOffsetInFile) {
        while (numRecordsBeforeEachOffsetInFile.hasNext()) {
          KV<Long, Long> entry = numRecordsBeforeEachOffsetInFile.next();
          if (entry.getKey().equals(offset)) {
            return entry.getValue();
          }
        }
        LOG.error("Unable to compute contextual metadata. Please report a bug in ContextualTextIO");
        return null;
      }
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<Row> getSource() {
      return CompressedSource.from(
              new ContextualTextIOSource(
                  getFilepattern(),
                  getMatchConfiguration().getEmptyMatchTreatment(),
                  getDelimiter(),
                  getHasMultilineCSVRecords()))
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
              DisplayData.item("hasMultilineCSVRecords", getHasMultilineCSVRecords())
                  .withLabel("Has RFC4180 MultiLineCSV Records"));
    }
  }

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<Row>> {
    abstract long getDesiredBundleSizeBytes();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    abstract byte @Nullable [] getDelimiter();

    abstract boolean getHasMultilineCSVRecords();

    abstract boolean getWithRecordNumMetadata();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder setHasMultilineCSVRecords(boolean hasMultilineCSVRecords);

      abstract Builder setWithRecordNumMetadata(boolean withRecordNumMetadata);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

      abstract ReadFiles build();
    }

    @VisibleForTesting
    ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @VisibleForTesting
    ReadFiles withRecordNumMetadata() {
      return toBuilder().setWithRecordNumMetadata(true).build();
    }

    /** Like {@link Read#withDelimiter}. */
    public ReadFiles withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    @Override
    public PCollection<Row> expand(PCollection<FileIO.ReadableFile> input) {

      PCollection<Row> rows =
          input.apply(
              "Read all via FileBasedSource",
              new ReadAllViaFileBasedSource<>(
                  getDesiredBundleSizeBytes(),
                  new CreateTextSourceFn(getDelimiter(), getHasMultilineCSVRecords()),
                  SchemaCoder.of(RecordWithMetadata.getSchema())));

      if (!getWithRecordNumMetadata()) {
        return rows;
      }

      return rows.apply(new ProcessRecordNumbers());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
              .withLabel("Custom delimiter to split records"));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<Row>> {
      private byte[] delimiter;
      private boolean hasMultilineCSVRecords;

      private CreateTextSourceFn(byte[] delimiter, boolean hasMultilineCSVRecords) {
        this.delimiter = delimiter;
        this.hasMultilineCSVRecords = hasMultilineCSVRecords;
      }

      @Override
      public FileBasedSource<Row> apply(String input) {
        return new ContextualTextIOSource(
            StaticValueProvider.of(input),
            EmptyMatchTreatment.DISALLOW,
            delimiter,
            hasMultilineCSVRecords);
      }
    }
  }

  /** Disable construction of utility class. */
  private ContextualTextIO() {}

  private static class ProcessRecordNumbers extends PTransform<PCollection<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollection<Row> records) {
      /*
       * At this point the line number in RecordWithMetadata contains the relative line offset from the beginning of the read range.
       *
       * To compute the absolute position from the beginning of the input we group the lines within the same ranges, and evaluate the size of each range.
       */

      // This algorithm only works with triggers that fire once, for now only default trigger is
      // supported.
      Trigger currentTrigger = records.getWindowingStrategy().getTrigger();

      Set<Trigger> allowedTriggers =
          ImmutableSet.of(
              Repeatedly.forever(AfterWatermark.pastEndOfWindow()), DefaultTrigger.of());

      Preconditions.checkArgument(
          allowedTriggers.contains(currentTrigger),
          String.format(
              "getWithRecordNumMetadata(true) only supports the default trigger not: %s",
              currentTrigger));

      PCollection<KV<KV<String, Long>, Row>> recordsGroupedByFileAndRange =
          records
              .apply("AddFileNameAndRange", ParDo.of(new AddFileNameAndRange()))
              .setCoder(
                  KvCoder.of(
                      KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()),
                      RowCoder.of(RecordWithMetadata.getSchema())));

      PCollectionView<Map<String, Iterable<KV<Long, Long>>>> rangeSizes =
          recordsGroupedByFileAndRange
              .apply("CountRecordsForEachFileRange", Count.perKey())
              .apply(
                  MapElements.into(
                          TypeDescriptors.kvs(
                              TypeDescriptors.strings(),
                              TypeDescriptors.kvs(
                                  TypeDescriptors.longs(), TypeDescriptors.longs())))
                      .<KV<KV<String, Long>, Long>>via(
                          x ->
                              KV.of(
                                  x.getKey().getKey(), KV.of(x.getKey().getValue(), x.getValue()))))
              .apply("SizesAsView", View.asMultimap());

      // Get Pipeline to create a dummy PCollection with one element to help compute the lines
      // before each Range
      PCollection<Integer> singletonPcoll =
          records.getPipeline().apply("CreateSingletonPcoll", Create.of(Arrays.asList(1)));

      /*
       * For each (File, Offset) pair, calculate the number of lines occurring before the Range for each file
       *
       * After computing the number of lines before each range, we can find the line number in original file as numLinesBeforeOffset + lineNumInCurrentOffset
       */

      PCollectionView<Map<String, Iterable<KV<Long, Long>>>> numRecordsBeforeEachRange =
          singletonPcoll
              .apply(
                  "ComputeNumRecordsBeforeRange",
                  ParDo.of(new ComputeRecordsBeforeEachRange(rangeSizes))
                      .withSideInputs(rangeSizes))
              .apply("NumRecordsBeforeEachRangeAsView", View.asMultimap());

      return recordsGroupedByFileAndRange
          .apply(
              "AssignLineNums",
              ParDo.of(new AssignRecordNums(numRecordsBeforeEachRange))
                  .withSideInputs(numRecordsBeforeEachRange))
          .setRowSchema(RecordWithMetadata.getSchema());
    }
  }
}
