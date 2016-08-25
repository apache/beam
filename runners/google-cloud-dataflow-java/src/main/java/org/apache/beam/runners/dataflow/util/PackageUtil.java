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

import com.fasterxml.jackson.core.Base64Variants;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.ZipFiles;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper routines for packages. */
public class PackageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PackageUtil.class);
  /**
   * A reasonable upper bound on the number of jars required to launch a Dataflow job.
   */
  public static final int SANE_CLASSPATH_SIZE = 1000;
  /**
   * The initial interval to use between package staging attempts.
   */
  private static final Duration INITIAL_BACKOFF_INTERVAL = Duration.standardSeconds(5);
  /**
   * The maximum number of retries when staging a file.
   */
  private static final int MAX_RETRIES = 4;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withMaxRetries(MAX_RETRIES).withInitialBackoff(INITIAL_BACKOFF_INTERVAL);

  /**
   * Translates exceptions from API calls.
   */
  private static final ApiErrorExtractor ERROR_EXTRACTOR = new ApiErrorExtractor();

  /**
   * Creates a DataflowPackage containing information about how a classpath element should be
   * staged, including the staging destination as well as its size and hash.
   *
   * @param classpathElement The local path for the classpath element.
   * @param stagingPath The base location for staged classpath elements.
   * @param overridePackageName If non-null, use the given value as the package name
   *                            instead of generating one automatically.
   * @return The package.
   */
  @Deprecated
  public static DataflowPackage createPackage(File classpathElement,
      String stagingPath, String overridePackageName) {
    return createPackageAttributes(classpathElement, stagingPath, overridePackageName)
        .getDataflowPackage();
  }

  /**
   * Compute and cache the attributes of a classpath element that we will need to stage it.
   *
   * @param classpathElement the file or directory to be staged.
   * @param stagingPath The base location for staged classpath elements.
   * @param overridePackageName If non-null, use the given value as the package name
   *                            instead of generating one automatically.
   * @return a {@link PackageAttributes} that containing metadata about the object to be staged.
   */
  static PackageAttributes createPackageAttributes(File classpathElement,
      String stagingPath, String overridePackageName) {
    try {
      boolean directory = classpathElement.isDirectory();

      // Compute size and hash in one pass over file or directory.
      Hasher hasher = Hashing.md5().newHasher();
      OutputStream hashStream = Funnels.asOutputStream(hasher);
      CountingOutputStream countingOutputStream = new CountingOutputStream(hashStream);

      if (!directory) {
        // Files are staged as-is.
        Files.asByteSource(classpathElement).copyTo(countingOutputStream);
      } else {
        // Directories are recursively zipped.
        ZipFiles.zipDirectory(classpathElement, countingOutputStream);
      }

      long size = countingOutputStream.getCount();
      String hash = Base64Variants.MODIFIED_FOR_URL.encode(hasher.hash().asBytes());

      // Create the DataflowPackage with staging name and location.
      String uniqueName = getUniqueContentName(classpathElement, hash);
      String resourcePath = IOChannelUtils.resolve(stagingPath, uniqueName);
      DataflowPackage target = new DataflowPackage();
      target.setName(overridePackageName != null ? overridePackageName : uniqueName);
      target.setLocation(resourcePath);

      return new PackageAttributes(size, hash, directory, target);
    } catch (IOException e) {
      throw new RuntimeException("Package setup failure for " + classpathElement, e);
    }
  }

  /**
   * Transfers the classpath elements to the staging location.
   *
   * @param classpathElements The elements to stage.
   * @param stagingPath The base location to stage the elements to.
   * @return A list of cloud workflow packages, each representing a classpath element.
   */
  public static List<DataflowPackage> stageClasspathElements(
      Collection<String> classpathElements, String stagingPath) {
    return stageClasspathElements(classpathElements, stagingPath, Sleeper.DEFAULT);
  }

  // Visible for testing.
  static List<DataflowPackage> stageClasspathElements(
      Collection<String> classpathElements, String stagingPath,
      Sleeper retrySleeper) {
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

    ArrayList<DataflowPackage> packages = new ArrayList<>();

    if (stagingPath == null) {
      throw new IllegalArgumentException(
          "Can't stage classpath elements on because no staging location has been provided");
    }

    int numUploaded = 0;
    int numCached = 0;
    for (String classpathElement : classpathElements) {
      String packageName = null;
      if (classpathElement.contains("=")) {
        String[] components = classpathElement.split("=", 2);
        packageName = components[0];
        classpathElement = components[1];
      }

      File file = new File(classpathElement);
      if (!file.exists()) {
        LOG.warn("Skipping non-existent classpath element {} that was specified.",
            classpathElement);
        continue;
      }

      PackageAttributes attributes = createPackageAttributes(file, stagingPath, packageName);

      DataflowPackage workflowPackage = attributes.getDataflowPackage();
      packages.add(workflowPackage);
      String target = workflowPackage.getLocation();

      // TODO: Should we attempt to detect the Mime type rather than
      // always using MimeTypes.BINARY?
      try {
        try {
          long remoteLength = IOChannelUtils.getSizeBytes(target);
          if (remoteLength == attributes.getSize()) {
            LOG.debug("Skipping classpath element already staged: {} at {}",
                classpathElement, target);
            numCached++;
            continue;
          }
        } catch (FileNotFoundException expected) {
          // If the file doesn't exist, it means we need to upload it.
        }

        // Upload file, retrying on failure.
        BackOff backoff = BACKOFF_FACTORY.backoff();
        while (true) {
          try {
            LOG.debug("Uploading classpath element {} to {}", classpathElement, target);
            try (WritableByteChannel writer = IOChannelUtils.create(target, MimeTypes.BINARY)) {
              copyContent(classpathElement, writer);
            }
            numUploaded++;
            break;
          } catch (IOException e) {
            if (ERROR_EXTRACTOR.accessDenied(e)) {
              String errorMessage = String.format(
                  "Uploaded failed due to permissions error, will NOT retry staging "
                  + "of classpath %s. Please verify credentials are valid and that you have "
                  + "write access to %s. Stale credentials can be resolved by executing "
                  + "'gcloud auth login'.", classpathElement, target);
              LOG.error(errorMessage);
              throw new IOException(errorMessage, e);
            }
            long sleep = backoff.nextBackOffMillis();
            if (sleep == BackOff.STOP) {
              // Rethrow last error, to be included as a cause in the catch below.
              LOG.error("Upload failed, will NOT retry staging of classpath: {}",
                  classpathElement, e);
              throw e;
            } else {
              LOG.warn("Upload attempt failed, sleeping before retrying staging of classpath: {}",
                  classpathElement, e);
              retrySleeper.sleep(sleep);
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Could not stage classpath element: " + classpathElement, e);
      }
    }

    LOG.info("Uploading PipelineOptions.filesToStage complete: {} files newly uploaded, "
        + "{} files cached",
        numUploaded, numCached);

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
    private DataflowPackage dataflowPackage;

    public PackageAttributes(long size, String hash, boolean directory,
        DataflowPackage dataflowPackage) {
      this.size = size;
      this.hash = Objects.requireNonNull(hash, "hash");
      this.directory = directory;
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
  }
}
