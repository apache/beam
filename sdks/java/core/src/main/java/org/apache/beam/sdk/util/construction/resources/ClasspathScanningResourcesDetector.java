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

import io.github.classgraph.ClassGraph;
import java.io.File;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Attempts to detect all the resources to be staged using classgraph library.
 *
 * <p>See <a
 * href="https://github.com/classgraph/classgraph">https://github.com/classgraph/classgraph</a>
 */
public class ClasspathScanningResourcesDetector implements PipelineResourcesDetector {

  private static final Object LOCK = new Object();
  private static @Nullable WeakReference<ClassLoader> cachedClassLoader;
  private static @Nullable String cachedJavaClassPath;
  private static @Nullable List<String> cachedResult;

  private transient ClassGraph classGraph;

  public ClasspathScanningResourcesDetector(ClassGraph classGraph) {
    this.classGraph = classGraph;
  }

  /**
   * Detects classpath resources and returns a list of absolute paths to them.
   *
   * @param classLoader The classloader to use to detect resources to stage (optional).
   * @return A list of absolute paths to the resources the class loader uses.
   */
  @Override
  public List<String> detect(@Nullable ClassLoader classLoader) {
    synchronized (LOCK) {
      String currentJavaClassPath = System.getProperty("java.class.path");
      if (cachedResult != null
          && Objects.equals(currentJavaClassPath, cachedJavaClassPath)
          && classLoader != null
          && cachedClassLoader != null
          && cachedClassLoader.get() == classLoader) {
        return new ArrayList<>(cachedResult);
      }

      List<File> classpathContents;
      if (classLoader != null) {
        classpathContents =
            classGraph.disableNestedJarScanning().addClassLoader(classLoader).getClasspathFiles();
      } else {
        classpathContents = classGraph.disableNestedJarScanning().getClasspathFiles();
      }

      List<String> result =
          classpathContents.stream().map(File::getAbsolutePath).collect(Collectors.toList());
      if (classLoader != null) {
        cachedClassLoader = new WeakReference<>(classLoader);
        cachedJavaClassPath = currentJavaClassPath;
        cachedResult = new ArrayList<>(result);
      }
      return result;
    }
  }
}
