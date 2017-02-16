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
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  public static final String DEFAULT_SCHEME = "default";

  private static final Map<String, FileSystemRegistrar> SCHEME_TO_REGISTRAR =
      new ConcurrentHashMap<>();

  private static PipelineOptions defaultConfig;

  static {
    loadFileSystemRegistrars();
  }

  /********************************** METHODS FOR CLIENT **********************************/

  /**
   * Returns a write channel for the given {@link ResourceId}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * @param resourceId the reference of the file-like resource to create
   * @param mimeType the mine type of the file-like resource to create
   */
  public static WritableByteChannel create(ResourceId resourceId, String mimeType)
      throws IOException {
    return create(resourceId, StandardCreateOptions.builder().setMimeType(mimeType).build());
  }

  /**
   * Returns a write channel for the given {@link ResourceId} with {@link CreateOptions}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * @param resourceId the reference of the file-like resource to create
   * @param createOptions the configuration of the create operation
   */
  public static WritableByteChannel create(ResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return getFileSystemInternal(resourceId).create(resourceId, createOptions);
  }

  /**
   * Returns a read channel for the given {@link ResourceId}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   *
   * @param resourceId the reference of the file-like resource to open
   */
  public static ReadableByteChannel open(ResourceId resourceId) throws IOException {
    return getFileSystemInternal(resourceId).open(resourceId);
  }

  /********************************** METHODS FOR REGISTRATION **********************************/

  /**
   * Loads available {@link FileSystemRegistrar} services.
   */
  private static void loadFileSystemRegistrars() {
    SCHEME_TO_REGISTRAR.clear();
    Set<FileSystemRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(Lists.newArrayList(
        ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader())));

    verifySchemesAreUnique(registrars);

    for (FileSystemRegistrar registrar : registrars) {
      SCHEME_TO_REGISTRAR.put(registrar.getScheme().toLowerCase(), registrar);
    }
  }

  /**
   * Sets the default configuration in workers.
   *
   * <p>It will be used in {@link FileSystemRegistrar FileSystemRegistrars} for all schemes.
   */
  public static void setDefaultConfigInWorkers(PipelineOptions options) {
    defaultConfig = checkNotNull(options, "options");
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(ResourceId resourceId) {
    String lowerCaseScheme = resourceId.getScheme().toLowerCase();
    return getRegistrarInternal(lowerCaseScheme).fromOptions(defaultConfig);
  }

  /**
   * Internal method to get {@link FileSystemRegistrar} for {@code scheme}.
   */
  @VisibleForTesting
  static FileSystemRegistrar getRegistrarInternal(String scheme) {
    String lowerCaseScheme = scheme.toLowerCase();
    if (SCHEME_TO_REGISTRAR.containsKey(lowerCaseScheme)) {
      return SCHEME_TO_REGISTRAR.get(lowerCaseScheme);
    } else if (SCHEME_TO_REGISTRAR.containsKey(DEFAULT_SCHEME)) {
      return SCHEME_TO_REGISTRAR.get(DEFAULT_SCHEME);
    } else {
      throw new IllegalStateException("Unable to find registrar for " + scheme);
    }
  }

  @VisibleForTesting
  static void verifySchemesAreUnique(Set<FileSystemRegistrar> registrars) {
    Multimap<String, FileSystemRegistrar> registrarsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (FileSystemRegistrar registrar : registrars) {
      registrarsBySchemes.put(registrar.getScheme().toLowerCase(), registrar);
    }
    for (Entry<String, Collection<FileSystemRegistrar>> entry
        : registrarsBySchemes.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        String conflictingRegistrars = Joiner.on(", ").join(
            FluentIterable.from(entry.getValue())
                .transform(new Function<FileSystemRegistrar, String>() {
                  @Override
                  public String apply(@Nonnull FileSystemRegistrar input) {
                    return input.getClass().getName();
                  }})
                .toSortedList(Ordering.<String>natural()));
        throw new IllegalStateException(String.format(
            "Scheme: [%s] has conflicting registrars: [%s]",
            entry.getKey(),
            conflictingRegistrars));
      }
    }
  }
}
