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
package org.apache.beam.sdk.util.construction.resources;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Funnels;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for working with classpath resources for pipelines. */
public class PipelineResources {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineResources.class);

  /**
   * Uses algorithm provided via {@link PipelineResourcesOptions} to detect classpath resources.
   *
   * @param classLoader The URLClassLoader to use to detect resources to stage (optional).
   * @param options pipeline options
   * @return A list of absolute paths to the resources the class loader uses.
   */
  public static List<String> detectClassPathResourcesToStage(
      ClassLoader classLoader, PipelineOptions options) {

    PipelineResourcesOptions artifactsRelatedOptions = options.as(PipelineResourcesOptions.class);
    List<String> detectedResources =
        artifactsRelatedOptions.getPipelineResourcesDetector().detect(classLoader);

    return detectedResources.stream().filter(isStageable()).collect(Collectors.toList());
  }

  /**
   * Returns a predicate for filtering all resources that are impossible to stage (like gradle
   * wrapper jars).
   */
  private static Predicate<String> isStageable() {
    return resourcePath -> !resourcePath.contains("gradle/wrapper");
  }

  /**
   * Goes through the list of files that need to be staged on runner. Removes nonexistent
   * directories and packages existing ones. This is necessary for runners that require filesToStage
   * to be jars only.
   *
   * <p>This method mutates the filesToStage value of the given options.
   *
   * @param options options object with the files to stage and temp location for staging
   */
  public static void prepareFilesForStaging(FileStagingOptions options) {
    List<String> filesToStage = options.getFilesToStage();
    if (filesToStage == null || filesToStage.isEmpty()) {
      filesToStage = detectClassPathResourcesToStage(ReflectHelpers.findClassLoader(), options);
      LOG.info(
          "PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged.",
          filesToStage.size());
      LOG.debug("Classpath elements: {}", filesToStage);
    }
    final String tmpJarLocation =
        MoreObjects.firstNonNull(options.getTempLocation(), System.getProperty("java.io.tmpdir"));
    final List<String> resourcesToStage = prepareFilesForStaging(filesToStage, tmpJarLocation);
    options.setFilesToStage(resourcesToStage);
  }

  /**
   * Goes through the list of files that need to be staged on the runner. If the file does not exist
   * on the specified location, it copies it to the temporary directory. This method now supports
   * both local file systems and HDFS.
   *
   * @param resourcesToStage list of resources that need to be staged
   * @param tmpJarLocation temporary directory to store the jars
   * @return A list of absolute paths to resources (jar files)
   * @throws RuntimeException if there is an error accessing the file system or staging the files
   */
  public static List<String> prepareFilesForStaging(
      List<String> resourcesToStage, String tmpJarLocation) {
    Configuration conf = new Configuration();
    FileSystem fs;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get FileSystem", e);
    }

    List<String> stagedFiles = new ArrayList<>();
    for (String resource : resourcesToStage) {
      Path srcPath = new Path(resource);
      Path destPath = new Path(tmpJarLocation, srcPath.getName());
      try {
        if (!fs.exists(destPath)) {
          fs.copyFromLocalFile(srcPath, destPath);
        }
        stagedFiles.add(destPath.toString());
      } catch (IOException e) {
        throw new RuntimeException("Error staging file: " + resource, e);
      }
    }
    return stagedFiles;
  }

  private static String packageDirectoriesToStage(File directoryToStage, String tmpJarLocation) {
    String hash = calculateDirectoryContentHash(directoryToStage);
    String pathForJar = getUniqueJarPath(hash, tmpJarLocation);
    try {
      ZipFiles.zipDirectoryOverwrite(directoryToStage, new File(pathForJar));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return pathForJar;
  }

  private static String calculateDirectoryContentHash(File directoryToStage) {
    Hasher hasher = Hashing.sha256().newHasher();
    try (OutputStream hashStream = Funnels.asOutputStream(hasher)) {
      ZipFiles.zipDirectory(directoryToStage, hashStream);
      return hasher.hash().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getUniqueJarPath(String contentHash, String tmpJarLocation) {
    checkArgument(
        !Strings.isNullOrEmpty(tmpJarLocation),
        "Please provide temporary location for storing the jar files.");

    return String.format("%s%s%s.jar", tmpJarLocation, File.separator, contentHash);
  }
}
