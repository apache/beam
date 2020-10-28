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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.ProviderNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.UdfProvider;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.commons.codec.digest.DigestUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads {@link UdfProvider} implementations from user-provided jars.
 *
 * <p>All UDFs are loaded and cached for each jar to mitigate IO costs.
 */
public class JavaUdfLoader {
  private static final Logger LOG = LoggerFactory.getLogger(JavaUdfLoader.class);

  /**
   * Maps the external jar location to the functions the jar defines. Static so it can persist
   * across multiple SQL transforms.
   */
  private static final Map<String, UserFunctionDefinitions> cache = new HashMap<>();

  private static ClassLoader originalClassLoader = null;

  /**
   * Load a user-defined scalar function from the specified jar.
   *
   * <p><strong>WARNING</strong>: The first time a jar is loaded, it is added to the thread's
   * context {@link ClassLoader} so that the jar can be staged by the runner.
   */
  public Method loadScalarFunction(List<String> functionPath, String jarPath) {
    String functionFullName = String.join(".", functionPath);
    try {
      UserFunctionDefinitions functionDefinitions = loadJar(jarPath);
      if (!functionDefinitions.javaScalarFunctions.containsKey(functionPath)) {
        throw new IllegalArgumentException(
            String.format(
                "No implementation of scalar function %s found in %s.%n"
                    + " 1. Create a class implementing %s and annotate it with @AutoService(%s.class).%n"
                    + " 2. Add function %s to the class's userDefinedScalarFunctions implementation.",
                functionFullName,
                jarPath,
                UdfProvider.class.getSimpleName(),
                UdfProvider.class.getSimpleName(),
                functionFullName));
      }
      return functionDefinitions.javaScalarFunctions.get(functionPath);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load user-defined scalar function %s from %s", functionFullName, jarPath),
          e);
    }
  }

  /**
   * Load a user-defined aggregate function from the specified jar.
   *
   * <p><strong>WARNING</strong>: The first time a jar is loaded, it is added to the thread's
   * context {@link ClassLoader} so that the jar can be staged by the runner.
   */
  public Combine.CombineFn loadAggregateFunction(List<String> functionPath, String jarPath) {
    String functionFullName = String.join(".", functionPath);
    try {
      UserFunctionDefinitions functionDefinitions = loadJar(jarPath);
      if (!functionDefinitions.javaAggregateFunctions.containsKey(functionPath)) {
        throw new IllegalArgumentException(
            String.format(
                "No implementation of aggregate function %s found in %s.%n"
                    + " 1. Create a class implementing %s and annotate it with @AutoService(%s.class).%n"
                    + " 2. Add function %s to the class's userDefinedAggregateFunctions implementation.",
                functionFullName,
                jarPath,
                UdfProvider.class.getSimpleName(),
                UdfProvider.class.getSimpleName(),
                functionFullName));
      }
      return functionDefinitions.javaAggregateFunctions.get(functionPath);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load user-defined aggregate function %s from %s",
              functionFullName, jarPath),
          e);
    }
  }

  private ClassLoader createAndSetClassLoader(String inputJarPath) throws IOException {
    Preconditions.checkArgument(!inputJarPath.isEmpty(), "Jar path cannot be empty.");
    ResourceId inputJar = FileSystems.matchNewResource(inputJarPath, false /* is directory */);
    File tmpJar = File.createTempFile("sql-udf-", inputJar.getFilename());
    FileSystems.copy(
        Collections.singletonList(inputJar),
        Collections.singletonList(
            FileSystems.matchNewResource(tmpJar.getAbsolutePath(), false /* is directory */)));
    try (InputStream inputStream = new FileInputStream(tmpJar)) {
      LOG.info(
          "Copied {} to {} with md5 hash {}.",
          inputJarPath,
          tmpJar.getAbsolutePath(),
          DigestUtils.md5Hex(inputStream));
    }
    if (originalClassLoader == null) {
      // Save the original context class loader before any UDF jars are loaded.
      originalClassLoader = Thread.currentThread().getContextClassLoader();
    }
    // Set the thread's context class loader so that the jar can be staged by the runner.
    Thread.currentThread()
        .setContextClassLoader(
            new URLClassLoader(
                new URL[] {tmpJar.toURI().toURL()},
                Thread.currentThread().getContextClassLoader()));
    // Return a class loader that isolates the target jar from other UDF jars that might have been
    // loaded previously.
    return new URLClassLoader(new URL[] {tmpJar.toURI().toURL()}, originalClassLoader);
  }

  @VisibleForTesting
  Iterator<UdfProvider> getUdfProviders(ClassLoader classLoader) throws IOException {
    return ServiceLoader.load(UdfProvider.class, classLoader).iterator();
  }

  private UserFunctionDefinitions loadJar(String jarPath) throws IOException {
    if (cache.containsKey(jarPath)) {
      LOG.debug("Using cached function definitions from {}", jarPath);
      return cache.get(jarPath);
    }

    Map<List<String>, Method> scalarFunctions = new HashMap<>();
    Map<List<String>, Combine.CombineFn> aggregateFunctions = new HashMap<>();
    ClassLoader classLoader = createAndSetClassLoader(jarPath);
    Iterator<UdfProvider> providers = getUdfProviders(classLoader);
    int providersCount = 0;
    while (providers.hasNext()) {
      providersCount++;
      UdfProvider provider = providers.next();
      provider
          .userDefinedScalarFunctions()
          .forEach(
              (functionName, implementation) -> {
                List<String> functionPath = ImmutableList.copyOf(functionName.split("\\."));
                if (scalarFunctions.containsKey(functionPath)) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Found multiple definitions of scalar function %s in %s.",
                          functionName, jarPath));
                }
                scalarFunctions.put(functionPath, implementation);
              });
      provider
          .userDefinedAggregateFunctions()
          .forEach(
              (functionName, implementation) -> {
                List<String> functionPath = ImmutableList.copyOf(functionName.split("\\."));
                if (aggregateFunctions.containsKey(functionPath)) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Found multiple definitions of aggregate function %s in %s.",
                          functionName, jarPath));
                }
                aggregateFunctions.put(functionPath, implementation);
              });
    }
    if (providersCount == 0) {
      throw new ProviderNotFoundException(
          String.format(
              "No %s implementation found in %s. Create a class implementing %s and annotate it with @AutoService(%s.class).",
              UdfProvider.class.getSimpleName(),
              jarPath,
              UdfProvider.class.getSimpleName(),
              UdfProvider.class.getSimpleName()));
    } else {
      LOG.info(
          "Loaded {} implementations of {} from {} with {} scalar function(s) and {} aggregate function(s).",
          providersCount,
          UdfProvider.class.getSimpleName(),
          jarPath,
          scalarFunctions.size(),
          aggregateFunctions.size());
    }
    UserFunctionDefinitions userFunctionDefinitions =
        new UserFunctionDefinitions(
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.copyOf(scalarFunctions),
            ImmutableMap.copyOf(aggregateFunctions));
    cache.put(jarPath, userFunctionDefinitions);
    return userFunctionDefinitions;
  }
}
