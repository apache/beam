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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.Base64Variants;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.Lists;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.GcsIOChannelFactory;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper routines for packages. */
class PackageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PackageUtil.class);
  /**
   * A reasonable upper bound on the number of jars required to launch a Dataflow job.
   */
  private static final int SANE_CLASSPATH_SIZE = 1000;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(4).withInitialBackoff(Duration.standardSeconds(5));

  /**
   * Translates exceptions from API calls.
   */
  private static final ApiErrorExtractor ERROR_EXTRACTOR = new ApiErrorExtractor();

  /**
   * Compute and cache the attributes of a classpath element that we will need to stage it.
   *
   * @param source the file or directory to be staged.
   * @param stagingPath The base location for staged classpath elements.
   * @param overridePackageName If non-null, use the given value as the package name
   *                            instead of generating one automatically.
   * @return a {@link PackageAttributes} that containing metadata about the object to be staged.
   */
  static PackageAttributes createPackageAttributes(File source,
      String stagingPath, @Nullable String overridePackageName) {
    boolean directory = source.isDirectory();

    // Compute size and hash in one pass over file or directory.
    Hasher hasher = Hashing.md5().newHasher();
    OutputStream hashStream = Funnels.asOutputStream(hasher);
    try (CountingOutputStream countingOutputStream = new CountingOutputStream(hashStream)) {
      if (!directory) {
        // Files are staged as-is.
        Files.asByteSource(source).copyTo(countingOutputStream);
      } else {
        // Directories are recursively zipped.
        ZipFiles.zipDirectory(source, countingOutputStream);
      }
      countingOutputStream.flush();

      long size = countingOutputStream.getCount();
      String hash = Base64Variants.MODIFIED_FOR_URL.encode(hasher.hash().asBytes());

      // Create the DataflowPackage with staging name and location.
      String uniqueName = getUniqueContentName(source, hash);
      String resourcePath = IOChannelUtils.resolve(stagingPath, uniqueName);
      DataflowPackage target = new DataflowPackage();
      target.setName(overridePackageName != null ? overridePackageName : uniqueName);
      target.setLocation(resourcePath);

      return new PackageAttributes(size, hash, directory, target, source.getPath());
    } catch (IOException e) {
      throw new RuntimeException("Package setup failure for " + source, e);
    }
  }

  /** Utility comparator used in uploading packages efficiently. */
  private static class PackageUploadOrder implements Comparator<PackageAttributes> {
    @Override
    public int compare(PackageAttributes o1, PackageAttributes o2) {
      // Smaller size compares high so that bigger packages are uploaded first.
      long sizeDiff = o2.getSize() - o1.getSize();
      if (sizeDiff != 0) {
        // returns sign of long
        return Long.signum(sizeDiff);
      }

      // Otherwise, choose arbitrarily based on hash.
      return o1.getHash().compareTo(o2.getHash());
    }
  }

  /**
   * Utility function that computes sizes and hashes of packages so that we can validate whether
   * they have already been correctly staged.
   */
  private static List<PackageAttributes> computePackageAttributes(
      Collection<String> classpathElements, final String stagingPath,
      ListeningExecutorService executorService) {
    List<ListenableFuture<PackageAttributes>> futures = new LinkedList<>();
    for (String classpathElement : classpathElements) {
      @Nullable String userPackageName = null;
      if (classpathElement.contains("=")) {
        String[] components = classpathElement.split("=", 2);
        userPackageName = components[0];
        classpathElement = components[1];
      }
      @Nullable final String packageName = userPackageName;

      final File file = new File(classpathElement);
      if (!file.exists()) {
        LOG.warn("Skipping non-existent classpath element {} that was specified.",
            classpathElement);
        continue;
      }

      ListenableFuture<PackageAttributes> future =
          executorService.submit(new Callable<PackageAttributes>() {
            @Override
            public PackageAttributes call() throws Exception {
              return createPackageAttributes(file, stagingPath, packageName);
            }
          });
      futures.add(future);
    }

    try {
      return Futures.allAsList(futures).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while staging packages", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Error while staging packages", e.getCause());
    }
  }

  private static WritableByteChannel makeWriter(String target, GcsUtil gcsUtil)
      throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(target);
    if (factory instanceof GcsIOChannelFactory) {
      return gcsUtil.create(GcsPath.fromUri(target), MimeTypes.BINARY);
    } else {
      return factory.create(target, MimeTypes.BINARY);
    }
  }

  /**
   * Utility to verify whether a package has already been staged and, if not, copy it to the
   * staging location.
   */
  private static void stageOnePackage(
      PackageAttributes attributes, AtomicInteger numUploaded, AtomicInteger numCached,
      Sleeper retrySleeper, GcsUtil gcsUtil) {
    String source = attributes.getSourcePath();
    String target = attributes.getDataflowPackage().getLocation();

    // TODO: Should we attempt to detect the Mime type rather than
    // always using MimeTypes.BINARY?
    try {
      try {
        long remoteLength = IOChannelUtils.getSizeBytes(target);
        if (remoteLength == attributes.getSize()) {
          LOG.debug("Skipping classpath element already staged: {} at {}",
              attributes.getSourcePath(), target);
          numCached.incrementAndGet();
          return;
        }
      } catch (FileNotFoundException expected) {
        // If the file doesn't exist, it means we need to upload it.
      }

      // Upload file, retrying on failure.
      BackOff backoff = BACKOFF_FACTORY.backoff();
      while (true) {
        try {
          LOG.debug("Uploading classpath element {} to {}", source, target);
          try (WritableByteChannel writer = makeWriter(target, gcsUtil)) {
            copyContent(source, writer);
          }
          numUploaded.incrementAndGet();
          break;
        } catch (IOException e) {
          if (ERROR_EXTRACTOR.accessDenied(e)) {
            String errorMessage = String.format(
                "Uploaded failed due to permissions error, will NOT retry staging "
                    + "of classpath %s. Please verify credentials are valid and that you have "
                    + "write access to %s. Stale credentials can be resolved by executing "
                    + "'gcloud auth application-default login'.", source, target);
            LOG.error(errorMessage);
            throw new IOException(errorMessage, e);
          }
          long sleep = backoff.nextBackOffMillis();
          if (sleep == BackOff.STOP) {
            // Rethrow last error, to be included as a cause in the catch below.
            LOG.error("Upload failed, will NOT retry staging of classpath: {}",
                source, e);
            throw e;
          } else {
            LOG.warn("Upload attempt failed, sleeping before retrying staging of classpath: {}",
                source, e);
            retrySleeper.sleep(sleep);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not stage classpath element: " + source, e);
    }
  }

  /**
   * Transfers the classpath elements to the staging location.
   *
   * @param classpathElements The elements to stage.
   * @param stagingPath The base location to stage the elements to.
   * @return A list of cloud workflow packages, each representing a classpath element.
   */
  static List<DataflowPackage> stageClasspathElements(
      Collection<String> classpathElements, String stagingPath, GcsUtil gcsUtil) {
    ListeningExecutorService executorService =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(32));
    try {
      return stageClasspathElements(
          classpathElements, stagingPath, Sleeper.DEFAULT, executorService, gcsUtil);
    } finally {
      executorService.shutdown();
    }
  }

  // Visible for testing.
  static List<DataflowPackage> stageClasspathElements(
      Collection<String> classpathElements, final String stagingPath,
      final Sleeper retrySleeper, ListeningExecutorService executorService, final GcsUtil gcsUtil) {
    LOG.info("Uploading {} files from PipelineOptions.filesToStage to staging location to "
        + "prepare for execution.", classpathElements.size());

    if (classpathElements.size() > SANE_CLASSPATH_SIZE) {
      LOG.warn("Your classpath contains {} elements, which Google Cloud Dataflow automatically "
            + "copies to all workers. Having this many entries on your classpath may be indicative "
            + "of an issue in your pipeline. You may want to consider trimming the classpath to "
            + "necessary dependencies only, using --filesToStage pipeline option to override "
            + "what files are being staged, or bundling several dependencies into one.",
          classpathElements.size());
    }

    checkArgument(
        stagingPath != null,
        "Can't stage classpath elements because no staging location has been provided");

    // Inline a copy here because the inner code returns an immutable list and we want to mutate it.
    List<PackageAttributes> packageAttributes =
        new LinkedList<>(computePackageAttributes(classpathElements, stagingPath, executorService));

    // Compute the returned list of DataflowPackage objects here so that they are returned in the
    // same order as on the classpath.
    List<DataflowPackage> packages = Lists.newArrayListWithExpectedSize(packageAttributes.size());
    for (final PackageAttributes attributes : packageAttributes) {
      packages.add(attributes.getDataflowPackage());
    }

    // Order package attributes in descending size order so that we upload the largest files first.
    Collections.sort(packageAttributes, new PackageUploadOrder());
    final AtomicInteger numUploaded = new AtomicInteger(0);
    final AtomicInteger numCached = new AtomicInteger(0);

    List<ListenableFuture<?>> futures = new LinkedList<>();
    for (final PackageAttributes attributes : packageAttributes) {
      futures.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          stageOnePackage(attributes, numUploaded, numCached, retrySleeper, gcsUtil);
        }
      }));
    }
    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while staging packages", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Error while staging packages", e.getCause());
    }

    LOG.info(
        "Staging files complete: {} files cached, {} files newly uploaded",
        numCached.get(), numUploaded.get());

    return packages;
  }

  /**
   * Returns a unique name for a file with a given content hash.
   *
   * <p>Directory paths are removed. Example:
   * <pre>
   * dir="a/b/c/d", contentHash="f000" => d-f000.jar
   * file="a/b/c/d.txt", contentHash="f000" => d-f000.txt
   * file="a/b/c/d", contentHash="f000" => d-f000
   * </pre>
   */
  static String getUniqueContentName(File classpathElement, String contentHash) {
    String fileName = Files.getNameWithoutExtension(classpathElement.getAbsolutePath());
    String fileExtension = Files.getFileExtension(classpathElement.getAbsolutePath());
    if (classpathElement.isDirectory()) {
      return fileName + "-" + contentHash + ".jar";
    } else if (fileExtension.isEmpty()) {
      return fileName + "-" + contentHash;
    }
    return fileName + "-" + contentHash + "." + fileExtension;
  }

  /**
   * Copies the contents of the classpathElement to the output channel.
   *
   * <p>If the classpathElement is a directory, a Zip stream is constructed on the fly,
   * otherwise the file contents are copied as-is.
   *
   * <p>The output channel is not closed.
   */
  private static void copyContent(String classpathElement, WritableByteChannel outputChannel)
      throws IOException {
    final File classpathElementFile = new File(classpathElement);
    if (classpathElementFile.isDirectory()) {
      ZipFiles.zipDirectory(classpathElementFile, Channels.newOutputStream(outputChannel));
    } else {
      Files.asByteSource(classpathElementFile).copyTo(Channels.newOutputStream(outputChannel));
    }
  }
  /**
   * Holds the metadata necessary to stage a file or confirm that a staged file has not changed.
   */
  static class PackageAttributes {
    private final boolean directory;
    private final long size;
    private final String hash;
    private final String sourcePath;
    private DataflowPackage dataflowPackage;

    public PackageAttributes(long size, String hash, boolean directory,
        DataflowPackage dataflowPackage, String sourcePath) {
      this.size = size;
      this.hash = Objects.requireNonNull(hash, "hash");
      this.directory = directory;
      this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath");
      this.dataflowPackage = Objects.requireNonNull(dataflowPackage, "dataflowPackage");
    }

    /**
     * @return the dataflowPackage
     */
    public DataflowPackage getDataflowPackage() {
      return dataflowPackage;
    }

    /**
     * @return the directory
     */
    public boolean isDirectory() {
      return directory;
    }

    /**
     * @return the size
     */
    public long getSize() {
      return size;
    }

    /**
     * @return the hash
     */
    public String getHash() {
      return hash;
    }

    /**
     * @return the file to be uploaded
     */
    public String getSourcePath() {
      return sourcePath;
    }
  }
}
