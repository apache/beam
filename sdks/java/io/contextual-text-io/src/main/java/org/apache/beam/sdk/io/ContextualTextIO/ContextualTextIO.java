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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
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
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

/**
 * {@link PTransform}s that read text files and collect contextual information of the elements in
 * the input.
 *
 * <h2>Reading from text files</h2>
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code
 * ContextualTextIO.read()}. To instantiate a transform use {@link
 * ContextualTextIO.Read#from(String)} and specify the path of the file(s) to be read.
 * Alternatively, if the filenames to be read are themselves in a {@link PCollection} you can use
 * {@link FileIO} to match them and {@link ContextualTextIO#readFiles()} to read them.
 *
 * <p>{@link #read} returns a {@link PCollection} of {@link LineContext LineContext}, each
 * corresponding to one line of an inout UTF-8 text file (split into lines delimited by '\n', '\r',
 * '\r\n', or specified delimiter see {@link ContextualTextIO.Read#withDelimiter})
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
 * // A simple Read of a local file (only runs locally when the filepath is on system):
 * PCollection<LineContext> lines = p.apply(ContextualTextIO.read().from("/local/path/to/file.txt"));
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
 * PCollection<LineContext> lines =
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
 * PCollection<LineContext> lines = p.apply(ContextualTextIO.read()
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
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<LineContext> lines = p.apply(ContextualTextIO.read()
 *     .from("/local/path/to/files/*.csv")
 *      .withHasRFC4180MultiLineColumn(true));
 * }</pre>
 *
 * <p>Example 5: reading while watching for new files
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<LineContext> lines = p.apply(FileIO.match()
 *      .filepattern("filepattern")
 *      .continuously(
 *        Duration.millis(100),
 *        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))))
 *      .apply(FileIO.readMatches())
 *      .apply(ContextualTextIO.readFiles());
 * }</pre>
 *
 * NOTE: Using {@link ContextualTextIO.Read#withHasRFC4180MultiLineColumn(boolean)} introduces a
 * performance penalty: when this option is enabled, the input cannot be split and read in parallel.
 *
 * <h3>Reading a very large number of files</h3>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link ContextualTextIO.Read#withHintMatchesManyFiles} for better
 * performance and scalability. Note that it may decrease performance if the filepattern matches
 * only a small number of files.
 */
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
   * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
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
    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract boolean getHintMatchesManyFiles();

    abstract Compression getCompression();

    abstract @Nullable Boolean getHasRFC4180MultiLineColumn();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    abstract @Nullable byte[] getDelimiter();

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
      checkArgument(delimiter != null, "delimiter can not be null");
      checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
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
      checkNotNull(
          getFilepattern(), "need to set the filepattern of a ContextualTextIO.Read transform");
      PCollection<LineContext> lines = null;
      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        lines = input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
      } else {
        // All other cases go through FileIO + ReadFiles
        lines =
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

      // At this point the line number in LineContext contains the relative line offset from the
      // beginning of the read range.

      // To compute the absolute position from the beginning of the input,
      // we group the lines within the same ranges, and evaluate the size of each range.

      // The following operations will assigns line numbers to all LineContext Objects

      PCollection<KV<KV<String, Long>, LineContext>> linesGroupedByFileAndRange =
          lines.apply("addFileNameAndRange", ParDo.of(new addFileNameAndRange()));

      PCollectionView<Map<KV<String, Long>, Long>> sizes =
          linesGroupedByFileAndRange
              .apply("countLinesForEachFileRange", Count.perKey())
              .apply("sizesAsView", View.asMap());

      // Get Pipeline to create a dummy PCollection with one element so that
      PCollection<Integer> dummyPcoll =
          input.getPipeline().apply("CreateDummyPcoll", Create.of(Arrays.asList(1)));

      // For each (File, Range) pair, calculate the number of lines occurring before the Range for
      // each File

      // After computing the number of lines before each range, we can find the line number in
      // original file as numLiesBeforeOffset + lineNumInCurrentOffset
      PCollectionView<Map<KV<String, Long>, Long>> sizesOrdered =
          dummyPcoll
              .apply(
                  "computeLinesBeforeRange",
                  ParDo.of(new computeLinesBeforeEachRange(sizes)).withSideInputs(sizes))
              .apply("", View.asMap());

      return linesGroupedByFileAndRange.apply(
          "assignLineNums",
          ParDo.of(new assignLineNums(sizesOrdered)).withSideInputs(sizesOrdered));
    }

    protected static class addFileNameAndRange
        extends DoFn<LineContext, KV<KV<String, Long>, LineContext>> {
      @ProcessElement
      public void processElement(
          @Element LineContext line, OutputReceiver<KV<KV<String, Long>, LineContext>> out) {
        out.output(KV.of(KV.of(line.getFile(), line.getRange().getRangeNum()), line));
      }
    }

    /** Helper class for computing Number of Lines preceding each Pair of (File, Range) */
    protected static class computeLinesBeforeEachRange
        extends DoFn<Integer, KV<KV<String, Long>, Long>> {
      private final PCollectionView<Map<KV<String, Long>, Long>> sizes;

      public computeLinesBeforeEachRange(PCollectionView<Map<KV<String, Long>, Long>> sizes) {
        this.sizes = sizes;
      }

      // Add custom comparator as KV<K, V> is not comparable by default
      private static class FileRangeComparator<K extends Comparable<K>, V extends Comparable<V>>
          implements Comparator<KV<K, V>> {
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
        // Get the Map Containing the size from side-input
        Map<KV<String, Long>, Long> sizeMap = p.sideInput(sizes);

        // The FileRange Pair must be sorted
        SortedMap<KV<String, Long>, Long> sorted = new TreeMap<>(new FileRangeComparator<>());

        // Initialize sorted map with values
        for (Map.Entry<KV<String, Long>, Long> entry : sizeMap.entrySet()) {
          sorted.put(entry.getKey(), entry.getValue());
        }

        // HashMap that tracks lines passed for each file
        Map<String, Long> pastLines = new HashMap<>();

        // For each (File, Range) Pair, compute the number of lines before it
        for (Map.Entry entry : sorted.entrySet()) {
          Long lines = (long) entry.getValue();
          KV<String, Long> FileRange = (KV<String, Long>) entry.getKey();
          String file = FileRange.getKey();
          Long linesBefore = 0L;
          if (pastLines.containsKey(file)) {
            linesBefore = pastLines.get(file);
          }
          p.output(KV.of(FileRange, linesBefore));
          pastLines.put(file, linesBefore + lines);
        }
      }
    }

    protected static class assignLineNums
        extends DoFn<KV<KV<String, Long>, LineContext>, LineContext> {
      PCollectionView<Map<KV<String, Long>, Long>> sizesOrdered;

      public assignLineNums(PCollectionView<Map<KV<String, Long>, Long>> sizesOrdered) {
        this.sizesOrdered = sizesOrdered;
      }

      @ProcessElement
      public void processElement(ProcessContext p) {
        Long Range = p.element().getKey().getValue();
        String File = p.element().getKey().getKey();
        LineContext line = p.element().getValue();
        Long linesLessThanThisRange = p.sideInput(sizesOrdered).get(KV.of(File, Range));
        LineContext newLine =
            LineContext.newBuilder()
                .setLine(line.getLine())
                .setLineNum(line.getRange().getRangeLineNum() + linesLessThanThisRange)
                .setFile(line.getFile())
                .setRange(line.getRange())
                .build();
        p.output(newLine);
      }
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
    abstract @Nullable byte[] getDelimiter();

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

    @Override
    public PCollection<LineContext> expand(PCollection<FileIO.ReadableFile> input) {
      SchemaCoder<LineContext> coder = null;
      try {
        coder = input.getPipeline().getSchemaRegistry().getSchemaCoder(LineContext.class);
      } catch (NoSuchSchemaException e) {
        System.out.println("No Coder!");
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
