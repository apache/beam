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
package org.apache.beam.sdk.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Provides utilities for creating read and write channels.
 */
public class IOChannelUtils {
  // TODO: add registration mechanism for adding new schemas.
  private static final Map<String, IOChannelFactory> FACTORY_MAP =
      Collections.synchronizedMap(new HashMap<String, IOChannelFactory>());

  private static final ClassLoader CLASS_LOADER = ReflectHelpers.findClassLoader();

  /**
   * Associates a scheme with an {@link IOChannelFactory}.
   *
   * <p>The given factory is used to construct read and write channels when
   * a URI is provided with the given scheme.
   *
   * <p>For example, when reading from "gs://bucket/path", the scheme "gs" is
   * used to lookup the appropriate factory.
   *
   * <p>{@link PipelineOptions} are required to provide dependencies and
   * pipeline level configuration to the individual {@link IOChannelFactory IOChannelFactories}.
   *
   * @throws IllegalStateException if multiple {@link IOChannelFactory IOChannelFactories}
   * for the same scheme are detected.
   */
  @VisibleForTesting
  public static void setIOFactoryInternal(
      String scheme,
      IOChannelFactory factory,
      boolean override) {
    if (!override && FACTORY_MAP.containsKey(scheme)) {
      throw new IllegalStateException(String.format(
          "Failed to register IOChannelFactory: %s. "
              + "Scheme: [%s] is already registered with %s, and override is not allowed.",
          FACTORY_MAP.get(scheme).getClass(),
          scheme,
          factory.getClass()));
    }
    FACTORY_MAP.put(scheme, factory);
  }

  /**
   * Deregisters the scheme and the associated {@link IOChannelFactory}.
   */
  @VisibleForTesting
  static void deregisterScheme(String scheme) {
    FACTORY_MAP.remove(scheme);
  }

  /**
   * Registers all {@link IOChannelFactory IOChannelFactories} from {@link ServiceLoader}.
   *
   * <p>{@link PipelineOptions} are required to provide dependencies and
   * pipeline level configuration to the individual {@link IOChannelFactory IOChannelFactories}.
   *
   * <p>Multiple {@link IOChannelFactory IOChannelFactories} for the same scheme are not allowed.
   *
   * @throws IllegalStateException if multiple {@link IOChannelFactory IOChannelFactories}
   * for the same scheme are detected.
   */
  public static void registerIOFactories(PipelineOptions options) {
    registerIOFactoriesInternal(options, false /* override */);
  }

  /**
   * Registers all {@link IOChannelFactory IOChannelFactories} from {@link ServiceLoader}.
   *
   * <p>This requires {@link PipelineOptions} to provide, e.g., credentials for GCS.
   *
   * <p>Override existing schemes is allowed.
   *
   * @deprecated This is currently to provide different configurations for tests and
   * is still public for IOChannelFactory redesign purposes.
   */
  @Deprecated
  @VisibleForTesting
  public static void registerIOFactoriesAllowOverride(PipelineOptions options) {
    registerIOFactoriesInternal(options, true /* override */);
  }

  private static void registerIOFactoriesInternal(
      PipelineOptions options, boolean override) {
    Set<IOChannelFactoryRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(Lists.newArrayList(
        ServiceLoader.load(IOChannelFactoryRegistrar.class, CLASS_LOADER)));

    checkDuplicateScheme(registrars);

    for (IOChannelFactoryRegistrar registrar : registrars) {
      setIOFactoryInternal(
          registrar.getScheme(),
          registrar.fromOptions(options),
          override);
    }
  }

  @VisibleForTesting
  static void checkDuplicateScheme(Set<IOChannelFactoryRegistrar> registrars) {
    Multimap<String, IOChannelFactoryRegistrar> registrarsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (IOChannelFactoryRegistrar registrar : registrars) {
      registrarsBySchemes.put(registrar.getScheme(), registrar);
    }
    for (Entry<String, Collection<IOChannelFactoryRegistrar>> entry
        : registrarsBySchemes.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        String conflictingRegistrars = Joiner.on(", ").join(
            FluentIterable.from(entry.getValue())
                .transform(new Function<IOChannelFactoryRegistrar, String>() {
                  @Override
                  public String apply(@Nonnull IOChannelFactoryRegistrar input) {
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

  /**
   * Creates a write channel for the given filename.
   */
  public static WritableByteChannel create(String filename, String mimeType)
      throws IOException {
    return getFactory(filename).create(filename, mimeType);
  }

  /**
   * Creates a read channel for the given filename.
   */
  public static ReadableByteChannel open(String filename)
      throws IOException {
    return getFactory(filename).open(filename);
  }

  /**
   * Creates a write channel for the given file components.
   *
   * <p>If numShards is specified, then a ShardingWritableByteChannel is
   * returned.
   *
   * <p>Shard numbers are 0 based, meaning they start with 0 and end at the
   * number of shards - 1.
   */
  public static WritableByteChannel create(String prefix, String shardTemplate,
      String suffix, int numShards, String mimeType) throws IOException {
    if (numShards == 1) {
      return create(DefaultFilenamePolicy.constructName(prefix, shardTemplate, suffix, 0, 1),
                    mimeType);
    }

    // It is the callers responsibility to close this channel.
    @SuppressWarnings("resource")
    ShardingWritableByteChannel shardingChannel =
        new ShardingWritableByteChannel();

    Set<String> outputNames = new HashSet<>();
    for (int i = 0; i < numShards; i++) {
      String outputName =
          DefaultFilenamePolicy.constructName(prefix, shardTemplate, suffix, i, numShards);
      if (!outputNames.add(outputName)) {
        throw new IllegalArgumentException(
            "Shard name collision detected for: " + outputName);
      }
      WritableByteChannel channel = create(outputName, mimeType);
      shardingChannel.addChannel(channel);
    }

    return shardingChannel;
  }

  /**
   * Returns the size in bytes for the given specification.
   *
   * <p>The specification is not expanded; it is used verbatim.
   *
   * <p>{@link FileNotFoundException} will be thrown if the resource does not exist.
   */
  public static long getSizeBytes(String spec) throws IOException {
    return getFactory(spec).getSizeBytes(spec);
  }

  private static final Pattern URI_SCHEME_PATTERN = Pattern.compile(
      "(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  /**
   * Returns the IOChannelFactory associated with an input specification.
   */
  public static IOChannelFactory getFactory(String spec) throws IOException {
    // The spec is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, which is so circumscribed as to be
    // very easy to extract with a regex.
    Matcher matcher = URI_SCHEME_PATTERN.matcher(spec);

    if (!matcher.matches()) {
      return FileIOChannelFactory.fromOptions(null);
    }

    String scheme = matcher.group("scheme");
    IOChannelFactory ioFactory = FACTORY_MAP.get(scheme);
    if (ioFactory != null) {
      return ioFactory;
    }

    throw new IOException("Unable to find handler for " + spec);
  }

  /**
   * Resolve multiple {@code others} against the {@code path} sequentially.
   *
   * <p>Empty paths in {@code others} are ignored. If {@code others} contains one or more
   * absolute paths, then this method returns a path that starts with the last absolute path
   * in {@code others} joined with the remaining paths. Resolution of paths is highly
   * implementation dependent and therefore unspecified.
   */
  public static String resolve(String path, String... others) throws IOException {
    IOChannelFactory ioFactory = getFactory(path);
    String fullPath = path;

    for (String other : others) {
      fullPath = ioFactory.resolve(fullPath, other);
    }

    return fullPath;
  }
}
