package org.apache.beam.sdk.io;

import com.google.auto.value.AutoValue;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * Transforms for accessing Delta Lake files: listing files (matching) and reading.
 *
 * <h2>Getting snapshot</h2>
 *
 * <p>{@link #snapshot} and {@link #matchAll} match filepatterns (respectively either a single
 * filepattern or a {@link PCollection} thereof) and return the files that match them as {@link
 * PCollection PCollections} of {@link MatchResult.Metadata}. Configuration options for them are in
 * {@link MatchConfiguration} and include features such as treatment of filepatterns that don't
 * match anything and continuous incremental matching of filepatterns (watching for new files).
 *
 * <h3>Example: Watching a Delta Lake for new files</h3>
 *
 * <p>This example matches a single filepattern repeatedly every 30 seconds, continuously returns
 * new matched files as an unbounded {@code PCollection<Metadata>} and stops if no new files appear
 * for 1 hour.
 *
 * <pre>{@code
 * PCollection<Metadata> matches = p.apply(DeltaFileIO.match()
 *     .filepattern("...")
 *     .continuously(
 *       Duration.standardSeconds(30), afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Example: Matching a Delta Lake directory for a specific version </h3>
 *
 * <p>This example reads Delta Lake parquet files for a version 5.
 *
 * <pre>{@code
 * PCollection<Metadata> matches = p.apply(DeltaFileIO.match()
 *     .filepattern("...")
 *     .withVersion(5L)
 * }</pre>
 *
 * <h2>Reading files</h2>
 *
 * <p>{@link #readMatches} converts each result of {@link #snapshot} or {@link #matchAll} to a {@link
 * ReadableFile} that is convenient for reading a file's contents, optionally decompressing it.
 *
 * <h3>Example: Returning filenames of parquet files from the latest snapshot in Delta Lake</h3>
 * *
 * <pre>{@code
     pipeline
      .apply("Get Snapshot",
        DeltaFileIO.snapshot()
            .filepattern(filePattern)
            .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
      )
      .apply("Read matched files",
            DeltaFileIO.readMatches()
      )
      .apply("Read parquet files",
          ParquetIO.readFiles(<SCHEMA>)
      )
 * }</pre>
 *
 */

@SuppressWarnings({
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class DeltaFileIO
{
  private static final Logger LOG = LoggerFactory.getLogger(DeltaFileIO.class);

  /**
   * Process a filepattern using DeltaLake Standalone and produces a collection of parquet files
   *  as {@link MatchResult.Metadata}.
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
  public static Match snapshot() {
    return new AutoValue_DeltaFileIO_Match.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * Like {@link #snapshot}, but matches each filepattern in a collection of filepatterns.
   *
   * <p>Resources are not deduplicated between filepatterns, i.e. if the same resource matches
   * multiple filepatterns, it will be produced multiple times.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#ALLOW_IF_WILDCARD}. To configure this behavior, use {@link
   * MatchAll#withEmptyMatchTreatment}.
   */
  public static MatchAll matchAll() {
    return new AutoValue_DeltaFileIO_MatchAll.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  /**
   * Converts each result of {@link #snapshot} or {@link #matchAll} to a {@link ReadableFile} which can
   * be used to read the contents of each file, optionally decompressing it.
   */
  public static ReadMatches readMatches() {
    return new AutoValue_DeltaFileIO_ReadMatches.Builder()
        .setCompression(Compression.AUTO)
        .setDirectoryTreatment(ReadMatches.DirectoryTreatment.SKIP)
        .build();
  }


  /**
   * Describes configuration for matching filepatterns, such as {@link EmptyMatchTreatment} and
   * continuous watching for matching files.
   */
  @AutoValue
  public abstract static class MatchConfiguration implements HasDisplayData, Serializable {
    /** Creates a {@link MatchConfiguration} with the given {@link EmptyMatchTreatment}. */
    public static MatchConfiguration create(EmptyMatchTreatment emptyMatchTreatment) {
      return new AutoValue_DeltaFileIO_MatchConfiguration.Builder()
          .setEmptyMatchTreatment(emptyMatchTreatment)
          .build();
    }

    public abstract EmptyMatchTreatment getEmptyMatchTreatment();

    public abstract @Nullable Duration getWatchInterval();

    abstract @Nullable TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract @Nullable Long getVersion();

    abstract @Nullable Long getTimestamp();

    abstract @Nullable SerializableConfiguration getHadoopConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setWatchInterval(Duration watchInterval);

      abstract Builder setWatchTerminationCondition(TerminationCondition<String, ?> condition);

      abstract Builder setVersion(Long version);

      abstract Builder setTimestamp(Long timestamp);

      abstract Builder setHadoopConfiguration(SerializableConfiguration hadoopConfiguration);

      abstract MatchConfiguration build();
    }

    /** Sets the {@link EmptyMatchTreatment}. */
    public MatchConfiguration withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    public MatchConfiguration withVersion(Long version) {
      return toBuilder().setVersion(version).build();
    }

    public MatchConfiguration withTimestamp(Long timestamp) {
      return toBuilder().setTimestamp(timestamp).build();
    }

    public MatchConfiguration withHadoopConfiguration(SerializableConfiguration hadoopConfiguration) {
      return toBuilder().setHadoopConfiguration(hadoopConfiguration).build();
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

  /** Implementation of {@link #snapshot}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>>
  {
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

    public Match withVersion(Long version) {
      return withConfiguration(getConfiguration().withVersion(version));
    }

    public Match withTimestamp(Long timestamp) {
      return withConfiguration(getConfiguration().withTimestamp(timestamp));
    }

    public Match withHadoopConfiguration(Configuration hadoopConfiguration) {
      return withConfiguration(getConfiguration()
          .withHadoopConfiguration(new SerializableConfiguration(hadoopConfiguration)));
    }

    /**
     * See {@link MatchConfiguration#continuously}. The returned {@link PCollection} is unbounded.
     *
     * <p>This works only in runners supporting splittable {@link
     * org.apache.beam.sdk.transforms.DoFn}.
     */
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Delta Create Filepattern",
              Create.ofProvider(getFilepattern(), StringUtf8Coder.of())
          )
          .apply("Delta Via MatchAll",
              matchAll().withConfiguration(getConfiguration())
          )
      ;
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
      extends PTransform<PCollection<String>, PCollection<MatchResult.Metadata>>
  {
    private static final Logger logger = LoggerFactory.getLogger(MatchAll.class);

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
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input)
    {
      logger.info("matchAll config: {}", getConfiguration());

      PCollection<MatchResult.Metadata> res;
      if (getConfiguration().getWatchInterval() == null) {
        res =
            input.apply(
                "Delta MatchAll FilePatterns",
                ParDo.of(new MatchOnceFn(getConfiguration())));
      } else {
        res =
            input.apply(
                    "Delta Continuously MatchAll filePatterns",
                    Watch.growthOf(
                            Contextful.of(new MatchPollFn(getConfiguration()), Requirements.empty()),
                            new ExtractFilenameFn())
                        .withPollInterval(getConfiguration().getWatchInterval())
                        .withTerminationPerInput(getConfiguration().getWatchTerminationCondition()))
                .apply(Values.create());
      }
      return res.apply("Delta Reshuffle files", Reshuffle.viaRandomKey());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include("configuration", getConfiguration());
    }

    private static class MatchOnceFn extends DoFn<String, MatchResult.Metadata>
    {
      private final MatchConfiguration matchConfiguration;

      MatchOnceFn(MatchConfiguration matchConfiguration) {
        this.matchConfiguration = matchConfiguration;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception
      {
        LOG.info("DeltaFileIO:MatchOnceFn processing {}", matchConfiguration);

        String filePattern = c.element();

        Configuration hadoopConfiguration = (matchConfiguration.getHadoopConfiguration() != null) ?
            matchConfiguration.getHadoopConfiguration().get() : new Configuration();

        DeltaLog deltaLog = getDeltaLog(filePattern, hadoopConfiguration);
        Snapshot deltaSnapshot = getDeltaSnapshot(deltaLog, matchConfiguration);

        List<AddFile> deltaFiles = deltaSnapshot.getAllFiles();

        LOG.info("DeltaFileIO:MatchOnceFn DeltaLog.forTables: version={}, numberOfFiles={}",
            deltaSnapshot.getVersion(), deltaFiles.size());

        String separator = filePattern.endsWith("/") ? "" : "/";

        for (AddFile file : deltaFiles) {
          String fullPath = filePattern + separator + file.getPath();
          MatchResult match = FileSystems.match(fullPath, matchConfiguration.getEmptyMatchTreatment());
          LOG.info("DeltaFileIO will process {}", match);
          for (MatchResult.Metadata metadata : match.metadata()) {
            c.output(metadata);
          }
        }
      }
    }

    static DeltaLog getDeltaLog(String filePattern, Configuration hadoopConfiguration)
    {
      // Delta standalone use Hadoop Filesystem, so need to replace s3 schema
      String deltaFilePattern = (filePattern.startsWith("s3://")) ?
          filePattern.replaceFirst("s3://", "s3a://") : filePattern;
      LOG.info("DeltaFileIO trying DeltaLog.forTables for pattern {}", deltaFilePattern);

      DeltaLog deltaLog = DeltaLog.forTable(hadoopConfiguration, deltaFilePattern);
      return deltaLog;
    }

    static Snapshot getDeltaSnapshot(DeltaLog deltaLog, MatchConfiguration matchConfiguration)
    {
      Snapshot deltaSnapshot;

      if (matchConfiguration.getVersion() != null) {
        logger.info("DeltaFileIO creating snapshot for version {}", matchConfiguration.getVersion());
        deltaSnapshot = deltaLog.getSnapshotForVersionAsOf(matchConfiguration.getVersion());
      } else if (matchConfiguration.getTimestamp() != null) {
        logger.info("DeltaFileIO creating snapshot for timestamp {}", matchConfiguration.getTimestamp());
        deltaSnapshot = deltaLog.getSnapshotForTimestampAsOf(matchConfiguration.getTimestamp());
      } else {
        logger.info("DeltaFileIO creating snapshot the latest version");
        deltaSnapshot = deltaLog.snapshot();
      }

      return deltaSnapshot;
    }

    private static class MatchPollFn extends PollFn<String, MatchResult.Metadata>
    {
      private final MatchConfiguration matchConfiguration;
      private final long pollDutationMs;

      private transient DeltaLog deltaLog = null;
      private transient long deltaSnapshotVersion = -1;

      MatchPollFn(MatchConfiguration matchConfiguration) {
        this.matchConfiguration = matchConfiguration;
        pollDutationMs =  matchConfiguration.getWatchInterval().getStandardSeconds() * 1000L;
        LOG.info("DeltaFileIO.MatchPollFn created, pollDutationMs=" + pollDutationMs);
      }

      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String filePattern, Context c) throws Exception
      {
        Instant now = Instant.now();

        List<MatchResult.Metadata> filesMetadata;

        Snapshot deltaSnapshot;
        if (deltaLog == null) {
          Configuration hadoopConfiguration = (matchConfiguration.getHadoopConfiguration() != null) ?
              matchConfiguration.getHadoopConfiguration().get() : new Configuration();
          deltaLog = getDeltaLog(filePattern, hadoopConfiguration);

          deltaSnapshot = getDeltaSnapshot(deltaLog, matchConfiguration);
        } else {
          deltaSnapshot = deltaLog.update();
        }

        if (deltaSnapshot.getVersion() == deltaSnapshotVersion) {
          // LOG.info("DeltaFileIO.MatchPollFn: No update for deltaSnapshotVersion={}", deltaSnapshotVersion);
          filesMetadata = Collections.EMPTY_LIST;
        } else {
          List<AddFile> deltaFiles = deltaSnapshot.getAllFiles();
          LOG.info("DeltaFileIO.MatchPollFn: DeltaLog.updates, version={}, numberOfFiles={}",
              deltaSnapshot.getVersion(), deltaFiles.size());

          String separator = filePattern.endsWith("/") ? "" : "/";
          filesMetadata = new ArrayList<>();
          for (AddFile file : deltaFiles) {
            String fullPath = filePattern + separator + file.getPath();
            MatchResult match = FileSystems.match(fullPath, matchConfiguration.getEmptyMatchTreatment());
            // LOG.info("DeltaFileIO.MatchPollFn will process {}", match);
            for (MatchResult.Metadata metadata : match.metadata()) {
              filesMetadata.add(metadata);
            }
          }

          deltaSnapshotVersion = deltaSnapshot.getVersion();
          LOG.info("DeltaFileIO.MatchPollFn: for deltaSnapshotVersion={} updating filesMetadata.size={}",
              deltaSnapshotVersion, filesMetadata.size());
        }

        return Watch.Growth.PollResult
            .incomplete(now, filesMetadata)
            .withWatermark(now);
      }
    }

    private static class ExtractFilenameFn implements SerializableFunction<MatchResult.Metadata, String>
    {
      @Override
      public String apply(MatchResult.Metadata input) {
        String fileName = input.resourceId().toString();
        return fileName;
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
      return input.apply(
          "Delta to Readable Files",
          ParDo.of(new ToReadableFileFn(this))
      );
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("compression", getCompression().toString()));
      builder.add(DisplayData.item("directoryTreatment", getDirectoryTreatment().toString()));
    }

    /**
     * @return True if metadata is a directory and directory Treatment is SKIP.
     * @throws IllegalArgumentException if metadata is a directory and directoryTreatment
     *     is Prohibited.
     * @throws UnsupportedOperationException if metadata is a directory and
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
     * # shouldSkipDirectory(org.apache.beam.sdk.io.fs.MatchResult.Metadata,
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

}
