/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.collect.TreeTraverser;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;

import com.fasterxml.jackson.core.Base64Variants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Helper routines for packages. */
public class PackageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PackageUtil.class);
  /**
   * The initial interval to use between package staging attempts.
   */
  private static final long INITIAL_BACKOFF_INTERVAL_MS = 5000L;
  /**
   * The maximum number of attempts when staging a file.
   */
  private static final int MAX_ATTEMPTS = 5;

  /**
   * Creates a DataflowPackage containing information about how a classpath element should be
   * staged.
   *
   * @param classpathElement The local path for the classpath element.
   * @param stagingDirectory The base location in GCS for staged classpath elements.
   * @param overridePackageName If non-null, use the given value as the package name
   *                            instead of generating one automatically.
   * @return The package.
   */
  public static DataflowPackage createPackage(String classpathElement,
      GcsPath stagingDirectory, String overridePackageName) {
    try {
      File file = new File(classpathElement);
      String contentHash = computeContentHash(file);

      // Drop the directory prefixes, and form the filename + hash + extension.
      String uniqueName = getUniqueContentName(file, contentHash);

      GcsPath stagingPath = stagingDirectory.resolve(uniqueName);

      DataflowPackage target = new DataflowPackage();
      target.setName(overridePackageName != null ? overridePackageName : uniqueName);
      target.setLocation(stagingPath.toResourceName());
      return target;
    } catch (IOException e) {
      throw new RuntimeException("Package setup failure for " + classpathElement, e);
    }
  }

  /**
   * Transfers the classpath elements to GCS.
   *
   * @param gcsUtil GCS utility.
   * @param classpathElements The elements to stage onto GCS.
   * @param gcsStaging The path on GCS to stage the classpath elements to.
   * @return A list of cloud workflow packages, each representing a classpath element.
   */
  public static List<DataflowPackage> stageClasspathElementsToGcs(
      GcsUtil gcsUtil,
      Collection<String> classpathElements,
      GcsPath gcsStaging) {
    return stageClasspathElementsToGcs(gcsUtil, classpathElements, gcsStaging, Sleeper.DEFAULT);
  }

  // Visible for testing.
  static List<DataflowPackage> stageClasspathElementsToGcs(
      GcsUtil gcsUtil,
      Collection<String> classpathElements,
      GcsPath gcsStaging,
      Sleeper retrySleeper) {
    LOG.info("Uploading {} files from PipelineOptions.filesToStage to GCS to prepare for execution "
        + "in the cloud.", classpathElements.size());
    ArrayList<DataflowPackage> packages = new ArrayList<>();

    if (gcsStaging == null) {
      throw new IllegalArgumentException(
          "Can't stage classpath elements on GCS because no GCS location has been provided");
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

      DataflowPackage workflowPackage = createPackage(
          classpathElement, gcsStaging, packageName);

      packages.add(workflowPackage);
      GcsPath target = GcsPath.fromResourceName(workflowPackage.getLocation());

      // TODO: Should we attempt to detect the Mime type rather than
      // always using MimeTypes.BINARY?
      try {
        long remoteLength = gcsUtil.fileSize(target);
        if (remoteLength >= 0 && remoteLength == getClasspathElementLength(classpathElement)) {
          LOG.debug("Skipping classpath element already on gcs: {} at {}",
              classpathElement, target);
          numCached++;
          continue;
        }

        // Upload file, retrying on failure.
        AttemptBoundedExponentialBackOff backoff = new AttemptBoundedExponentialBackOff(
            MAX_ATTEMPTS,
            INITIAL_BACKOFF_INTERVAL_MS);
        while (true) {
          try {
            LOG.debug("Uploading classpath element {} to {}", classpathElement, target);
            try (WritableByteChannel writer = gcsUtil.create(target, MimeTypes.BINARY)) {
              copyContent(classpathElement, writer);
            }
            numUploaded++;
            break;
          } catch (IOException e) {
            if (!backoff.atMaxAttempts()) {
              LOG.warn("Upload attempt failed, sleeping before retrying staging of classpath: {}",
                  classpathElement, e);
              BackOffUtils.next(retrySleeper, backoff);
            } else {
              // Rethrow last error, to be included as a cause in the catch below.
              LOG.error("Upload failed, will NOT retry staging of classpath: {}",
                  classpathElement, e);
              throw e;
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
   * If classpathElement is a file, then the files length is returned, otherwise the length
   * of the copied stream is returned.
   *
   * @param classpathElement The local path for the classpath element.
   * @return The length of the classpathElement.
   */
  private static long getClasspathElementLength(String classpathElement) throws IOException {
    File file = new File(classpathElement);
    if (file.isFile()) {
      return file.length();
    }

    CountingOutputStream countingOutputStream =
        new CountingOutputStream(ByteStreams.nullOutputStream());
    try (WritableByteChannel channel = Channels.newChannel(countingOutputStream)) {
      copyContent(classpathElement, channel);
    }
    return countingOutputStream.getCount();
  }

  /**
   * Returns a unique name for a file with a given content hash.
   * <p>
   * Directory paths are removed. Example:
   * <pre>
   * dir="a/b/c/d", contentHash="f000" => d-f000.zip
   * file="a/b/c/d.txt", contentHash="f000" => d-f000.txt
   * file="a/b/c/d", contentHash="f000" => d-f000
   * </pre>
   */
  static String getUniqueContentName(File classpathElement, String contentHash) {
    String fileName = Files.getNameWithoutExtension(classpathElement.getAbsolutePath());
    String fileExtension = Files.getFileExtension(classpathElement.getAbsolutePath());
    if (classpathElement.isDirectory()) {
      return fileName + "-" + contentHash + ".zip";
    } else if (fileExtension.isEmpty()) {
      return fileName + "-" + contentHash;
    }
    return fileName + "-" + contentHash + "." + fileExtension;
  }

  /**
   * Computes a message digest of the file/directory contents, returning a base64 string which is
   * suitable for use in URLs.
   */
  private static String computeContentHash(File classpathElement) throws IOException {
    TreeTraverser<File> files = Files.fileTreeTraverser();
    Hasher hasher = Hashing.md5().newHasher();
    for (File currentFile : files.preOrderTraversal(classpathElement)) {
      String relativePath = relativize(currentFile, classpathElement);
      hasher.putString(relativePath, StandardCharsets.UTF_8);
      if (currentFile.isDirectory()) {
        hasher.putLong(-1L);
        continue;
      }
      hasher.putLong(currentFile.length());
      Files.asByteSource(currentFile).copyTo(Funnels.asOutputStream(hasher));
    }
    return Base64Variants.MODIFIED_FOR_URL.encode(hasher.hash().asBytes());
  }

  /**
   * Copies the contents of the classpathElement to the output channel.
   * <p>
   * If the classpathElement is a directory, a Zip stream is constructed on the fly,
   * otherwise the file contents are copied as-is.
   * <p>
   * The output channel is not closed.
   */
  private static void copyContent(String classpathElement, WritableByteChannel outputChannel)
      throws IOException {
    final File classpathElementFile = new File(classpathElement);
    if (classpathElementFile.isDirectory()) {
      zipDirectory(classpathElementFile, Channels.newOutputStream(outputChannel));
    } else {
      Files.asByteSource(classpathElementFile).copyTo(Channels.newOutputStream(outputChannel));
    }
  }

  /**
   * Zips an entire directory specified by the path.
   *
   * @param sourceDirectory the directory to read from. This directory and all
   *     subdirectories will be added to the zip-file. The path within the zip
   *     file is relative to the directory given as parameter, not absolute.
   * @param outputStream the stream to write the zip-file to. This method does not close
   *     outputStream.
   * @throws IOException the zipping failed, e.g. because the input was not
   *     readable.
   */
  private static void zipDirectory(
      File sourceDirectory,
      OutputStream outputStream) throws IOException {
    checkNotNull(sourceDirectory);
    checkNotNull(outputStream);
    checkArgument(
        sourceDirectory.isDirectory(),
        "%s is not a valid directory",
        sourceDirectory.getAbsolutePath());
    ZipOutputStream zos = new ZipOutputStream(outputStream);
    for (File file : sourceDirectory.listFiles()) {
      zipDirectoryInternal(file, "", zos);
    }
    zos.finish();
  }

  /**
   * Private helper function for zipping files. This one goes recursively
   * through the input directory and all of its subdirectories and adds the
   * single zip entries.
   *
   * @param inputFile the file or directory to be added to the zip file
   * @param directoryName the string-representation of the parent directory
   *     name. Might be an empty name, or a name containing multiple directory
   *     names separated by "/". The directory name must be a valid name
   *     according to the file system limitations.
   * @param zos the zipstream to write to
   * @throws IOException the zipping failed, e.g. because the output was not
   *     writeable.
   */
  private static void zipDirectoryInternal(
      File inputFile,
      String directoryName,
      ZipOutputStream zos) throws IOException {
    final String entryName;
    if ("".equals(directoryName)) {
      // no parent directories yet.
      entryName = inputFile.getName();
    } else {
      entryName = directoryName + "/" + inputFile.getName();
    }
    if (inputFile.isDirectory()) {
      // We are hitting a sub-directory. Start the recursion.
      // Add the empty entry for a subdirectory if we have no children files.
      // Don't add it if we have them, as this is incompatible with certain
      // implementations of unzip.
      if (inputFile.list().length == 0) {
        ZipEntry entry = new ZipEntry(entryName + "/");
        zos.putNextEntry(entry);
      } else {
        // loop through the directory content, and zip the files
        for (File file : inputFile.listFiles()) {
          zipDirectoryInternal(file, entryName, zos);
        }
      }
    } else {
      // Put the next zip-entry into the zipoutputstream.
      ZipEntry entry = new ZipEntry(entryName);
      zos.putNextEntry(entry);
      Files.asByteSource(inputFile).copyTo(zos);
    }
  }

  /**
   * Constructs a relative path between file and root.
   * <p>
   * This function will attempt to use {@link java.nio.file.Path#relativize} and
   * will fallback to using {@link java.net.URI#relativize} in AppEngine.
   *
   * @param file The file for which the relative path is being constructed for.
   * @param root The root from which the relative path should be constructed.
   * @return The relative path between the file and root.
   */
  private static String relativize(File file, File root) {
    if (AppEngineEnvironment.IS_APP_ENGINE) {
      // AppEngine doesn't allow for java.nio.file.Path to be used so we rely on
      // using URIs, but URIs are broken for UNC paths which AppEngine doesn't
      // use. See for more details: http://wiki.eclipse.org/Eclipse/UNC_Paths
      return root.toURI().relativize(file.toURI()).getPath();
    }
    return root.toPath().relativize(file.toPath()).toString();
  }
}
