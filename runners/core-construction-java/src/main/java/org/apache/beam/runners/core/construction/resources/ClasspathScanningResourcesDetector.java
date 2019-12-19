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

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attempts to detect all the resources to be staged using either URLClassLoader (if it is
 * available) or via the "java.class.path" system property. URLClassLoader is not available for Java
 * 9 and above, hence the alternative approach was introduced.
 */
public class ClasspathScanningResourcesDetector implements PipelineResourcesDetector {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClasspathScanningResourcesDetector.class);

  /**
   * Detects classpath resources by either using URLClassLoader or java.class.path env variable.
   *
   * @param classLoader The classloader to use to detect resources to stage (optional).
   * @throws IllegalArgumentException If one of the resources the class loader exposes is not a file
   *     resource.
   * @return A list of absolute paths to the resources the class loader uses.
   */
  @Override
  public List<String> detect(ClassLoader classLoader) {
    if (classLoader instanceof URLClassLoader) {
      return scanClasspathForResourcesToStage((URLClassLoader) classLoader);
    } else {
      return scanClasspathForResourcesToStage();
    }
  }

  private static List<String> scanClasspathForResourcesToStage() {
    LOG.info("Scanning classpath for resources to stage via the java.class.path system property.");

    Iterable<String> classpathEntries =
        Splitter.on(File.pathSeparator).split(System.getProperty("java.class.path"));

    return StreamSupport.stream(classpathEntries.spliterator(), false)
        .map(File::new)
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());
  }

  private static List<String> scanClasspathForResourcesToStage(URLClassLoader classLoader) {
    LOG.info("Scanning classpath for resources to stage via the URLClassLoader.");

    List<String> files = new ArrayList<>();
    for (URL url : classLoader.getURLs()) {
      try {
        files.add(new File(url.toURI()).getAbsolutePath());
      } catch (IllegalArgumentException | URISyntaxException e) {
        String message = String.format("Unable to convert url (%s) to file.", url);
        throw new IllegalArgumentException(message, e);
      }
    }
    return files;
  }
}
