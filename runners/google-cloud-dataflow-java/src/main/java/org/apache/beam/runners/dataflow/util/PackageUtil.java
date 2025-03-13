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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.extensions.gcp.storage.GcsCreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper routines for packages. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PackageUtil implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PackageUtil.class);

  /** A reasonable upper bound on the number of jars required to launch a Dataflow job. */
  private static final int SANE_CLASSPATH_SIZE = 1000;

  private static final int DEFAULT_THREAD_POOL_SIZE = 32;

  private static final Sleeper DEFAULT_SLEEPER = Sleeper.DEFAULT;

  private static final CreateOptions DEFAULT_CREATE_OPTIONS =
      GcsCreateOptions.builder()
          .setGcsUploadBufferSizeBytes(1024 * 1024)
          .setMimeType(MimeTypes.BINARY)
          .build();

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(4).withInitialBackoff(Duration.standardSeconds(5));

  /** Translates exceptions from API calls. */
  private static final ApiErrorExtractor ERROR_EXTRACTOR = new ApiErrorExtractor();

  private final ExecutorService executorService;

  private PackageUtil(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public static PackageUtil withDefaultThreadPool() {
    return PackageUtil.withExecutorService(
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                DEFAULT_THREAD_POOL_SIZE, MoreExecutors.platformThreadFactory())));
  }

  public static PackageUtil withExecutorService(ExecutorService executorService) {
    return new PackageUtil(executorService);
  }

  @Override
  public void close() {
    executorService.shutdown();
  }

  /** Asynchronously computes {@link PackageAttributes} for a single staged file. */
  private CompletionStage<PackageAttributes> computePackageAttributes(
      final String source, final String hash, final String dest, final String stagingPath) {

    return MoreFutures.supplyAsync(
        () -> PackageAttributes.forFileToStage(source, hash, dest, stagingPath), executorService);
  }

  private boolean alreadyStaged(PackageAttributes attributes) throws IOException {
    try {
      long remoteLength =
          FileSystems.matchSingleFileSpec(attributes.getDestination().getLocation()).sizeBytes();
      return remoteLength == attributes.getSize();
    } catch (FileNotFoundException expected) {
      // If the file doesn't exist, it means we need to upload it.
      return false;
    }
  }

  /** Stages one file ("package") if necessary. */
  public CompletionStage<StagingResult> stagePackage(
      final PackageAttributes attributes,
      final Sleeper retrySleeper,
      final CreateOptions createOptions) {
    return MoreFutures.supplyAsync(
        () -> stagePackageSynchronously(attributes, retrySleeper, createOptions), executorService);
  }

  /** Synchronously stages a package, with retry and backoff for resiliency. */
  private StagingResult stagePackageSynchronously(
      PackageAttributes attributes, Sleeper retrySleeper, CreateOptions createOptions)
      throws IOException, InterruptedException {
    String sourceDescription = attributes.getSourceDescription();
    String target = attributes.getDestination().getLocation();

    try {
      return tryStagePackageWithRetry(attributes, retrySleeper, createOptions);
    } catch (Exception miscException) {
      throw new RuntimeException(
          String.format("Could not stage %s to %s", sourceDescription, target), miscException);
    }
  }

  private StagingResult tryStagePackageWithRetry(
      PackageAttributes attributes, Sleeper retrySleeper, CreateOptions createOptions)
      throws IOException, InterruptedException {
    String sourceDescription = attributes.getSourceDescription();
    String target = attributes.getDestination().getLocation();
    BackOff backoff = BackOffAdapter.toGcpBackOff(BACKOFF_FACTORY.backoff());

    while (true) {
      if (alreadyStaged(attributes)) {
        LOG.debug("Skipping file already staged: {} at {}", sourceDescription, target);
        return StagingResult.cached(attributes);
      }

      try {
        return tryStagePackage(attributes, createOptions);
      } catch (IOException ioException) {

        if (ERROR_EXTRACTOR.accessDenied(ioException)) {
          String errorMessage =
              String.format(
                  "Uploaded failed due to permissions error, will NOT retry staging "
                      + "of %s. Please verify credentials are valid and that you have "
                      + "write access to %s. Stale credentials can be resolved by executing "
                      + "'gcloud auth application-default login'.",
                  sourceDescription, target);
          LOG.error(errorMessage);
          throw new IOException(errorMessage, ioException);
        }

        long sleep = backoff.nextBackOffMillis();
        if (sleep == BackOff.STOP) {
          LOG.error(
              "Upload failed, will NOT retry staging of package: {}",
              sourceDescription,
              ioException);
          throw new RuntimeException(
              String.format("Could not stage %s to %s", sourceDescription, target), ioException);
        } else {
          LOG.warn(
              "Upload attempt failed, sleeping before retrying staging of package: {}",
              sourceDescription,
              ioException);
          retrySleeper.sleep(sleep);
        }
      }
    }
  }

  private StagingResult tryStagePackage(PackageAttributes attributes, CreateOptions createOptions)
      throws IOException, InterruptedException {
    String sourceDescription = attributes.getSourceDescription();
    String target = attributes.getDestination().getLocation();

    LOG.info("Uploading {} to {}", sourceDescription, target);
    try (WritableByteChannel writer =
        FileSystems.create(FileSystems.matchNewResource(target, false), createOptions)) {
      if (attributes.getBytes() != null) {
        ByteSource.wrap(attributes.getBytes()).copyTo(Channels.newOutputStream(writer));
      } else {
        File sourceFile = attributes.getSource();
        checkState(
            sourceFile != null,
            "Internal inconsistency: we tried to stage something to %s, but neither a source file "
                + "nor the byte content was specified",
            target);
        Files.asByteSource(sourceFile).copyTo(Channels.newOutputStream(writer));
      }
    }
    return StagingResult.uploaded(attributes);
  }

  /**
   * Transfers the classpath elements to the staging location using a default {@link Sleeper}.
   *
   * @see #stageClasspathElements(Collection, String, Sleeper, CreateOptions)
   */
  List<DataflowPackage> stageClasspathElements(
      Collection<StagedFile> classpathElements, String stagingPath, CreateOptions createOptions) {
    return stageClasspathElements(classpathElements, stagingPath, DEFAULT_SLEEPER, createOptions);
  }

  /**
   * Transfers the classpath elements to the staging location using default settings.
   *
   * @see #stageClasspathElements(Collection, String, Sleeper, CreateOptions)
   */
  List<DataflowPackage> stageClasspathElements(
      Collection<StagedFile> classpathElements, String stagingPath) {
    return stageClasspathElements(
        classpathElements, stagingPath, DEFAULT_SLEEPER, DEFAULT_CREATE_OPTIONS);
  }

  public DataflowPackage stageToFile(
      byte[] bytes, String target, String stagingPath, CreateOptions createOptions) {
    try {
      return MoreFutures.get(
              stagePackage(
                  PackageAttributes.forBytesToStage(bytes, target, stagingPath),
                  DEFAULT_SLEEPER,
                  createOptions))
          .getPackageAttributes()
          .getDestination();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while staging pipeline", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Error while staging pipeline", e.getCause());
    }
  }

  /**
   * Transfers the classpath elements to the staging location.
   *
   * @param classpathElements The elements to stage.
   * @param stagingPath The base location to stage the elements to.
   * @return A list of cloud workflow packages, each representing a classpath element.
   */
  List<DataflowPackage> stageClasspathElements(
      Collection<StagedFile> classpathElements,
      final String stagingPath,
      final Sleeper retrySleeper,
      final CreateOptions createOptions) {
    LOG.info(
        "Uploading {} files from PipelineOptions.filesToStage to staging location to "
            + "prepare for execution.",
        classpathElements.size());
    Instant start = Instant.now();

    if (classpathElements.size() > SANE_CLASSPATH_SIZE) {
      LOG.warn(
          "Your classpath contains {} elements, which Google Cloud Dataflow automatically "
              + "copies to all workers. Having this many entries on your classpath may be indicative "
              + "of an issue in your pipeline. You may want to consider trimming the classpath to "
              + "necessary dependencies only, using --filesToStage pipeline option to override "
              + "what files are being staged, or bundling several dependencies into one.",
          classpathElements.size());
    }

    checkArgument(
        stagingPath != null,
        "Can't stage classpath elements because no staging location has been provided");

    final AtomicInteger numUploaded = new AtomicInteger(0);
    final AtomicInteger numCached = new AtomicInteger(0);
    List<CompletionStage<DataflowPackage>> destinationPackages = new ArrayList<>();
    final Set<String> distinctDestinations = Sets.newConcurrentHashSet();

    for (StagedFile classpathElement : classpathElements) {
      String dest = classpathElement.getDestination();
      String source = classpathElement.getSource();
      String hash = classpathElement.getSha256();

      File sourceFile = new File(source);
      if (!sourceFile.exists()) {
        LOG.warn("Skipping non-existent file to stage {}.", sourceFile);
        continue;
      }

      CompletionStage<StagingResult> stagingResult =
          computePackageAttributes(source, hash, dest, stagingPath)
              .thenComposeAsync(
                  packageAttributes -> {
                    String destLocation = packageAttributes.getDestination().getLocation();
                    if (distinctDestinations.add(destLocation)) {
                      return stagePackage(packageAttributes, retrySleeper, createOptions);
                    } else {
                      LOG.debug("Upload of {} skipped because it was already queued", destLocation);

                      return CompletableFuture.completedFuture(
                          StagingResult.cached(packageAttributes));
                    }
                  });

      CompletionStage<DataflowPackage> stagedPackage =
          stagingResult.thenApply(
              stagingResult1 -> {
                if (stagingResult1.alreadyStaged()) {
                  numCached.incrementAndGet();
                } else {
                  numUploaded.incrementAndGet();
                }
                return stagingResult1.getPackageAttributes().getDestination();
              });

      destinationPackages.add(stagedPackage);
    }

    try {
      CompletionStage<List<DataflowPackage>> stagingFutures =
          MoreFutures.allAsList(destinationPackages);
      boolean finished = false;
      do {
        try {
          MoreFutures.get(stagingFutures, 3L, TimeUnit.MINUTES);
          finished = true;
        } catch (TimeoutException e) {
          // finished will still be false
          LOG.info("Still staging {} files", classpathElements.size());
        }
      } while (!finished);
      List<DataflowPackage> stagedPackages = MoreFutures.get(stagingFutures);
      Instant done = Instant.now();
      LOG.info(
          "Staging files complete: {} files cached, {} files newly uploaded in {} seconds",
          numCached.get(),
          numUploaded.get(),
          Seconds.secondsBetween(start, done).getSeconds());
      return stagedPackages;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while staging packages", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Error while staging packages", e.getCause());
    }
  }

  @AutoValue
  public abstract static class StagedFile {
    public static PackageUtil.StagedFile of(String source, String sha256, String destination) {
      return new AutoValue_PackageUtil_StagedFile(source, sha256, destination);
    }

    /** The file to stage. */
    public abstract String getSource();
    /** The SHA-256 hash of the source file. */
    public abstract String getSha256();
    /** Staged target for this file. */
    public abstract String getDestination();
  }

  @AutoValue
  abstract static class StagingResult {
    abstract PackageAttributes getPackageAttributes();

    abstract boolean alreadyStaged();

    public static StagingResult cached(PackageAttributes attributes) {
      return new AutoValue_PackageUtil_StagingResult(attributes, true);
    }

    public static StagingResult uploaded(PackageAttributes attributes) {
      return new AutoValue_PackageUtil_StagingResult(attributes, false);
    }
  }

  /** Holds the metadata necessary to stage a file or confirm that a staged file has not changed. */
  @AutoValue
  abstract static class PackageAttributes {
    public static PackageAttributes forFileToStage(
        String source, String hash, String dest, String stagingPath) throws IOException {
      final File file = new File(source);
      if (!file.exists()) {
        throw new FileNotFoundException(
            String.format("Non-existent file to stage: %s", file.getAbsolutePath()));
      }
      checkState(!file.isDirectory(), "Source file must not be a directory.");
      String target;
      // Dataflow worker jar and windmill binary can be overridden by providing files with
      // predefined file names. Normally, we can use the artifact file name as same as
      // the last component of GCS object resource path. However, we need special handling
      // for those predefined names since they also need to be unique even in the same
      // staging directory.
      switch (dest) {
        case "dataflow-worker.jar":
        case "windmill_main":
          target =
              Environments.createStagingFileName(
                  file, Files.asByteSource(file).hash(Hashing.sha256()));
          LOG.info("Staging custom {} as {}", dest, target);
          break;
        default:
          target = dest;
      }
      DataflowPackage destination = new DataflowPackage();
      String resourcePath =
          FileSystems.matchNewResource(stagingPath, true)
              .resolve(target, StandardResolveOptions.RESOLVE_FILE)
              .toString();
      destination.setLocation(resourcePath);
      destination.setName(dest);
      return new AutoValue_PackageUtil_PackageAttributes(
          file, null, destination, file.length(), hash);
    }

    public static PackageAttributes forBytesToStage(
        byte[] bytes, String targetName, String stagingPath) {
      HashCode hashCode = Hashing.sha256().newHasher().putBytes(bytes).hash();
      long size = bytes.length;

      String target = Environments.createStagingFileName(new File(targetName), hashCode);

      String resourcePath =
          FileSystems.matchNewResource(stagingPath, true)
              .resolve(target, StandardResolveOptions.RESOLVE_FILE)
              .toString();
      DataflowPackage targetPackage = new DataflowPackage();
      targetPackage.setName(target);
      targetPackage.setLocation(resourcePath);

      return new AutoValue_PackageUtil_PackageAttributes(
          null, bytes, targetPackage, size, hashCode.toString());
    }

    public PackageAttributes withPackageName(String overridePackageName) {
      DataflowPackage newDestination = new DataflowPackage();
      newDestination.setName(overridePackageName);
      newDestination.setLocation(getDestination().getLocation());

      return new AutoValue_PackageUtil_PackageAttributes(
          getSource(), getBytes(), newDestination, getSize(), getHash());
    }

    /** @return the file to be uploaded, if any */
    public abstract @Nullable File getSource();

    /** @return the bytes to be uploaded, if any */
    @SuppressWarnings("mutable")
    public abstract byte @Nullable [] getBytes();

    /** @return the dataflowPackage */
    public abstract DataflowPackage getDestination();

    /** @return the size */
    public abstract long getSize();

    /** @return the hash */
    public abstract String getHash();

    public String getSourceDescription() {
      if (getSource() != null) {
        return getSource().toString();
      } else {
        return String.format("<%s bytes, hash %s>", getSize(), getHash());
      }
    }
  }
}
