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
import java.util.Collections;
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

  private static final class CachedClasspath {
    private final WeakReference<ClassLoader> classLoader;
    private final @Nullable String javaClassPath;
    private final List<String> files;

    CachedClasspath(ClassLoader classLoader, @Nullable String javaClassPath, List<String> files) {
      this.classLoader = new WeakReference<>(classLoader);
      this.javaClassPath = javaClassPath;
      this.files = Collections.unmodifiableList(new ArrayList<>(files));
    }

    boolean matches(@Nullable ClassLoader loader, @Nullable String currentJavaClassPath) {
      return loader != null
          && classLoader.get() == loader
          && Objects.equals(javaClassPath, currentJavaClassPath);
    }
  }

  private static final Object LOCK = new Object();
  private static volatile @Nullable CachedClasspath cachedClasspath;

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
    String currentJavaClassPath = System.getProperty("java.class.path");
    CachedClasspath snapshot = cachedClasspath;
    if (snapshot != null && snapshot.matches(classLoader, currentJavaClassPath)) {
      return new ArrayList<>(snapshot.files);
    }

    synchronized (LOCK) {
      currentJavaClassPath = System.getProperty("java.class.path");
      snapshot = cachedClasspath;
      if (snapshot != null && snapshot.matches(classLoader, currentJavaClassPath)) {
        return new ArrayList<>(snapshot.files);
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
        cachedClasspath = new CachedClasspath(classLoader, currentJavaClassPath, result);
      }
      return result;
    }
  }
}
