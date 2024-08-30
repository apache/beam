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

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

/** This returns a row count estimation for files associated with a file pattern. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class TextRowCountEstimator {
  private static final long DEFAULT_NUM_BYTES_PER_FILE = 64 * 1024L;
  private static final Compression DEFAULT_COMPRESSION = Compression.AUTO;
  private static final FileIO.ReadMatches.DirectoryTreatment DEFAULT_DIRECTORY_TREATMENT =
      FileIO.ReadMatches.DirectoryTreatment.SKIP;
  private static final EmptyMatchTreatment DEFAULT_EMPTY_MATCH_TREATMENT =
      EmptyMatchTreatment.DISALLOW;
  private static final SamplingStrategy DEFAULT_SAMPLING_STRATEGY = new SampleAllFiles();

  public abstract long getNumSampledBytesPerFile();

  @SuppressWarnings("mutable")
  public abstract byte @Nullable [] getDelimiters();

  public abstract int getSkipHeaderLines();

  public abstract String getFilePattern();

  public abstract Compression getCompression();

  public abstract SamplingStrategy getSamplingStrategy();

  public abstract EmptyMatchTreatment getEmptyMatchTreatment();

  public abstract FileIO.ReadMatches.DirectoryTreatment getDirectoryTreatment();

  public static TextRowCountEstimator.Builder builder() {
    return new AutoValue_TextRowCountEstimator.Builder()
        .setSamplingStrategy(DEFAULT_SAMPLING_STRATEGY)
        .setNumSampledBytesPerFile(DEFAULT_NUM_BYTES_PER_FILE)
        .setCompression(DEFAULT_COMPRESSION)
        .setDirectoryTreatment(DEFAULT_DIRECTORY_TREATMENT)
        .setEmptyMatchTreatment(DEFAULT_EMPTY_MATCH_TREATMENT)
        .setSkipHeaderLines(0);
  }

  /**
   * Estimates the number of non empty rows. It samples NumSampledBytesPerFile bytes from every file
   * until the condition in sampling strategy is met. Then it takes the average line size of the
   * rows and divides the total file sizes by that number. If all the sampled rows are empty, and it
   * has not sampled all the lines (due to sampling strategy) it throws Exception.
   *
   * @return Number of estimated rows.
   * @throws NoEstimationException if all the sampled lines are empty and we have not read all the
   *     lines in the matched files.
   */
  @SuppressFBWarnings(
      value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
      justification = "https://github.com/spotbugs/spotbugs/issues/756")
  public Double estimateRowCount(PipelineOptions pipelineOptions)
      throws IOException, NoEstimationException {
    long linesSize = 0;
    int numberOfReadLines = 0;
    long totalFileSizes = 0;
    long totalSampledBytes = 0;
    int numberOfReadFiles = 0;
    boolean sampledEverything = true;

    MatchResult match = FileSystems.match(getFilePattern(), getEmptyMatchTreatment());

    for (MatchResult.Metadata metadata : match.metadata()) {

      if (getSamplingStrategy().stopSampling(numberOfReadFiles, totalSampledBytes)) {
        sampledEverything = false;
        break;
      }

      if (FileIO.ReadMatches.shouldSkipDirectory(metadata, getDirectoryTreatment())) {
        continue;
      }

      FileIO.ReadableFile file = FileIO.ReadMatches.matchToReadableFile(metadata, getCompression());

      // We use this as an estimate of the size of the sampled lines. Since the last sampled line
      // may exceed this range, we are over estimating the number of lines in our estimation. (If
      // each line is larger than readingWindowSize we will read one line any way and that line is
      // the last line)
      long readingWindowSize = Math.min(getNumSampledBytesPerFile(), metadata.sizeBytes());
      sampledEverything = metadata.sizeBytes() == readingWindowSize && sampledEverything;
      OffsetRange range = new OffsetRange(0, readingWindowSize);

      TextSource textSource =
          new TextSource(
              ValueProvider.StaticValueProvider.of(file.getMetadata().resourceId().toString()),
              getEmptyMatchTreatment(),
              getDelimiters(),
              getSkipHeaderLines());
      FileBasedSource<String> source =
          CompressedSource.from(textSource).withCompression(file.getCompression());
      try (BoundedSource.BoundedReader<String> reader =
          source
              .createForSubrangeOfFile(file.getMetadata(), range.getFrom(), range.getTo())
              .createReader(pipelineOptions)) {

        int numberOfNonEmptyLines = 0;
        for (boolean more = reader.start(); more; more = reader.advance()) {
          numberOfNonEmptyLines += reader.getCurrent().trim().equals("") ? 0 : 1;
        }
        numberOfReadLines += numberOfNonEmptyLines;
        linesSize += (numberOfNonEmptyLines == 0) ? 0 : readingWindowSize;
      }
      long fileSize = metadata.sizeBytes();
      numberOfReadFiles += fileSize == 0 ? 0 : 1;
      totalFileSizes += fileSize;
    }

    if (numberOfReadLines == 0 && sampledEverything) {
      return 0d;
    }

    if (numberOfReadLines == 0) {
      throw new NoEstimationException(
          "Cannot estimate the row count. All the sampled lines are empty");
    }

    // This is total file sizes divided by average line size.
    return (double) totalFileSizes * numberOfReadLines / linesSize;
  }

  /** Builder for {@link org.apache.beam.sdk.io.TextRowCountEstimator}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setNumSampledBytesPerFile(long numSampledBytes);

    public abstract Builder setDirectoryTreatment(
        FileIO.ReadMatches.DirectoryTreatment directoryTreatment);

    public abstract Builder setCompression(Compression compression);

    public abstract Builder setDelimiters(byte @Nullable [] delimiters);

    public abstract Builder setSkipHeaderLines(int skipHeaderLines);

    public abstract Builder setFilePattern(String filePattern);

    public abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment emptyMatchTreatment);

    public abstract Builder setSamplingStrategy(SamplingStrategy samplingStrategy);

    public abstract TextRowCountEstimator build();
  }

  /**
   * An exception that will be thrown if the estimator cannot get an estimation of the number of
   * lines.
   */
  public static class NoEstimationException extends Exception {
    NoEstimationException(String message) {
      super(message);
    }
  }

  /** Sampling Strategy shows us when should we stop reading further files. * */
  public interface SamplingStrategy {
    boolean stopSampling(int numberOfFiles, long totalReadBytes);
  }

  /** This strategy samples all the files. */
  public static class SampleAllFiles implements SamplingStrategy {

    @Override
    public boolean stopSampling(int numberOfSampledFiles, long totalReadBytes) {
      return false;
    }
  }

  /** This strategy stops sampling if we sample enough number of bytes. */
  public static class LimitNumberOfFiles implements SamplingStrategy {
    int limit;

    public LimitNumberOfFiles(int limit) {
      this.limit = limit;
    }

    @Override
    public boolean stopSampling(int numberOfFiles, long totalReadBytes) {
      return numberOfFiles > limit;
    }
  }

  /**
   * This strategy stops sampling when total number of sampled bytes are more than some threshold.
   */
  public static class LimitNumberOfTotalBytes implements SamplingStrategy {
    long limit;

    public LimitNumberOfTotalBytes(long limit) {
      this.limit = limit;
    }

    @Override
    public boolean stopSampling(int numberOfFiles, long totalReadBytes) {
      return totalReadBytes > limit;
    }
  }
}
