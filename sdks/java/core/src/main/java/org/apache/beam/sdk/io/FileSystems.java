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

import static com.google.common.base.Preconditions.checkArgument;
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
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  public static final String DEFAULT_SCHEME = "default";

  private static final Pattern URI_SCHEME_PATTERN = Pattern.compile("^[a-zA-Z][-a-zA-Z0-9+.]*$");

  private static final ClassLoader CLASS_LOADER = ReflectHelpers.findClassLoader();

  private static final Map<String, FileSystemRegistrar> REGISTRAR_MAP =
      Collections.synchronizedMap(new HashMap<String, FileSystemRegistrar>());

  private static final Map<String, PipelineOptions> REGISTRAR_DEFAULT_CONFIG_MAP =
      Collections.synchronizedMap(new HashMap<String, PipelineOptions>());

  public static void loadFileSystemRegistrars() {
    REGISTRAR_MAP.clear();
    Set<FileSystemRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(Lists.newArrayList(
        ServiceLoader.load(FileSystemRegistrar.class, CLASS_LOADER)));

    checkDuplicateScheme(registrars);

    for (FileSystemRegistrar registrar : registrars) {
      addFileSystemRegistrar(registrar);
    }
  }

  /**
   * Sets the default config for {@link FileSystemRegistrar} that associates with {@code scheme}.
   *
   * <p>Syntax: scheme = alpha *( alpha | digit | "+" | "-" | "." )
   * Upper case letters are treated as the same as lower case letters.
   */
  public static void setDefaultConfig(String scheme, PipelineOptions options) {
    checkArgument(
        URI_SCHEME_PATTERN.matcher(scheme).matches(),
        String.format("Scheme: [%s] doesn't match URI syntax: %s",
            scheme, URI_SCHEME_PATTERN.pattern()));
    REGISTRAR_DEFAULT_CONFIG_MAP.put(
        checkNotNull(scheme, "scheme"), checkNotNull(options, "options"));
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  public static FileSystem getFileSystemInternal(URI uri) {
    String scheme =
        (uri.getScheme() != null ? uri.getScheme() : LocalFileSystemRegistrar.LOCAL_FILE_SCHEME);
    return getRegistrarInternal(scheme).fromOptions(REGISTRAR_DEFAULT_CONFIG_MAP.get(scheme));
  }

  /**
   * Internal method to get {@link FileSystemRegistrar} for {@code scheme}.
   */
  @VisibleForTesting
  static FileSystemRegistrar getRegistrarInternal(String scheme) {
    if (REGISTRAR_MAP.containsKey(scheme)) {
      return REGISTRAR_MAP.get(scheme);
    } else if (REGISTRAR_MAP.containsKey(DEFAULT_SCHEME)) {
      return REGISTRAR_MAP.get(DEFAULT_SCHEME);
    } else {
      throw new IllegalStateException("Unable to find handler for " + scheme);
    }
  }

  /**
   * Adds the {@link FileSystemRegistrar} to the static {@code REGISTRAR_MAP}.
   *
   * @throws IllegalStateException if multiple {@link FileSystem FileSystems}
   * for the same scheme are detected.
   */
  @VisibleForTesting
  private static void addFileSystemRegistrar(FileSystemRegistrar registrar) {
    if (REGISTRAR_MAP.containsKey(registrar.getScheme())) {
      throw new IllegalStateException(String.format(
          "Failed to register FileSystem: %s. "
              + "Scheme: [%s] is already registered with %s, and override is not allowed.",
          REGISTRAR_MAP.get(registrar.getScheme()).getClass(),
          registrar.getScheme(),
          registrar.getClass()));
    }
    REGISTRAR_MAP.put(registrar.getScheme(), registrar);
  }

  @VisibleForTesting
  static void checkDuplicateScheme(Set<FileSystemRegistrar> registrars) {
    Multimap<String, FileSystemRegistrar> registrarsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (FileSystemRegistrar registrar : registrars) {
      registrarsBySchemes.put(registrar.getScheme(), registrar);
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
