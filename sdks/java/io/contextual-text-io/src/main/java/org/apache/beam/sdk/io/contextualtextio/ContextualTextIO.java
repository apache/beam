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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <h2>Reading text files</h2>
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a {@link
 * PCollection} you can use {@link FileIO} to match them and {@link TextIO#readFiles} to read them.
 *
 * <p>{@link #read} returns a {@link PCollection} of {@link String Strings}, each corresponding to
 * one line of an input UTF-8 text file (split into lines delimited by '\n', '\r', or '\r\n', or
 * specified delimiter see {@link TextIO.Read#withDelimiter}).
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, the filepatterns are expanded only once. {@link Read#watchForNewFiles} or the
 * combination of {@link FileIO.Match#continuously(Duration, TerminationCondition)} and {@link
 * #readFiles()} allow streaming of new files matching the filepattern(s).
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readFiles()}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * Read#withEmptyMatchTreatment} or {@link
 * FileIO.Match#withEmptyMatchTreatment(EmptyMatchTreatment)} plus {@link #readFiles()} to configure
 * this behavior.
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
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
 * PCollection<String> lines =
 *     filenames
 *         .apply(FileIO.matchAll())
 *         .apply(FileIO.readMatches())
 *         .apply(TextIO.readFiles());
 * }</pre>
 *
 * <p>Example 3: streaming new files matching a filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> lines = p.apply(TextIO.read()
 *     .from("/local/path/to/files/*")
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
 * <h2>Writing text files</h2>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link TextIO.Write#to(String)} to specify the output prefix of the files to write.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"))
 *      .withSuffix(".txt")
 *      .withCompression(Compression.GZIP));
 * }</pre>
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 *
 * <p>If you want better control over how filenames are generated than the default policy allows, a
 * custom {@link FilenamePolicy} can also be set using {@link TextIO.Write#to(FilenamePolicy)}.
 *
 * <h3>Advanced features</h3>
 *
 * <p>{@link TextIO} supports all features of {@link FileIO#write} and {@link FileIO#writeDynamic},
 * such as writing windowed/unbounded data, writing data to multiple destinations, and so on, by
 * providing a {@link Sink} via {@link #sink()}.
 *
 * <p>For example, to write events of different type to different filenames:
 *
 * <pre>{@code
 * PCollection<Event> events = ...;
 * events.apply(FileIO.<EventType, Event>writeDynamic()
 *       .by(Event::getTypeName)
 *       .via(TextIO.sink(), Event::toString)
 *       .to(type -> nameFilesUsingWindowPaneAndShard(".../events/" + type + "/data", ".txt")));
 * }</pre>
 *
 * <p>For backwards compatibility, {@link TextIO} also supports the legacy {@link
 * DynamicDestinations} interface for advanced features via {@link Write#to(DynamicDestinations)}.
 */
public class ContextualTextIO {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded {@link
   * PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_ContextualTextIO_Read.Builder()
        .setCompression(Compression.AUTO)
        .setHintMatchesManyFiles(false)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
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
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   */

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract boolean getHintMatchesManyFiles();

    abstract Compression getCompression();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    abstract byte @Nullable [] getDelimiter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Builder setCompression(Compression compression);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

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
    public PCollection<String> expand(PBegin input) {
      checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
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
    protected FileBasedSource<String> getSource() {
      return CompressedSource.from(
              new ContextualTextIOSource(
                  getFilepattern(),
                  getMatchConfiguration().getEmptyMatchTreatment(),
                  getDelimiter()))
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
                  .withLabel("Custom delimiter to split records"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>> {
    abstract long getDesiredBundleSizeBytes();

    @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
    abstract byte @Nullable [] getDelimiter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

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
    public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {
      return input.apply(
          "Read all via FileBasedSource",
          new ReadAllViaFileBasedSource<>(
              getDesiredBundleSizeBytes(),
              new CreateTextSourceFn(getDelimiter()),
              StringUtf8Coder.of()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
              .withLabel("Custom delimiter to split records"));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<String>> {
      private byte[] delimiter;

      private CreateTextSourceFn(byte[] delimiter) {
        this.delimiter = delimiter;
      }

      @Override
      public FileBasedSource<String> apply(String input) {
        return new ContextualTextIOSource(
            StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW, delimiter);
      }
    }
  }
  /** Disable construction of utility class. */
  private ContextualTextIO() {}
}
