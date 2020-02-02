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
package org.apache.beam.runners.core.construction.resources;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnels;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/** Utilities for working with classpath resources for pipelines. */
public class PipelineResources {

  /**
   * Uses algorithm provided via {@link
   * org.apache.beam.runners.core.construction.resources.PipelineResourcesOptions} to detect
   * classpath resources.
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
   * @param resourcesToStage list of resources that need to be staged
   * @param tmpJarLocation temporary directory to store the jars
   * @return A list of absolute paths to resources (jar files)
   */
  public static List<String> prepareFilesForStaging(
      List<String> resourcesToStage, String tmpJarLocation) {
    return resourcesToStage.stream()
        .map(File::new)
        .map(
            file -> {
              Preconditions.checkState(
                  file.exists(), "To-be-staged file does not exist: '%s'", file);
              return file.isDirectory()
                  ? packageDirectoriesToStage(file, tmpJarLocation)
                  : file.getAbsolutePath();
            })
        .collect(Collectors.toList());
  }

  private static String packageDirectoriesToStage(File directoryToStage, String tmpJarLocation) {
    String hash = calculateDirectoryContentHash(directoryToStage);
    String pathForJar = getUniqueJarPath(hash, tmpJarLocation);
    zipDirectory(directoryToStage, pathForJar);
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

  private static void zipDirectory(File directoryToStage, String uniqueDirectoryPath) {
    try {
      ZipFiles.zipDirectory(directoryToStage, new FileOutputStream(uniqueDirectoryPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
