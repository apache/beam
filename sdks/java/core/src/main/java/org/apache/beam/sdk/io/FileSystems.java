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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.commons.lang3.SystemUtils;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  public static final String DEFAULT_SCHEME = "default";

  // Regex to parse the scheme.
  private static final Pattern URI_SCHEME_PARSING_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  // Regex to parse the URI (defined in https://tools.ietf.org/html/rfc3986#appendix-B).
  private static final Pattern URI_PARSING_PATTERN = Pattern.compile(
      "^((?<scheme>[^:/?#]+):)?(//(?<authority>[^/?#]*))?(?<path>[^?#]*)"
          + "(\\?(?<query>[^#]*))?(#(?<fragment>.*))?");

  private static final Map<String, FileSystemRegistrar> SCHEME_TO_REGISTRAR =
      new ConcurrentHashMap<>();

  private static PipelineOptions defaultConfig;

  private static final Map<String, PipelineOptions> SCHEME_TO_DEFAULT_CONFIG =
      new ConcurrentHashMap<>();

  static {
    loadFileSystemRegistrars();
  }

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

  private static final String URI_DELIMITER = "/";

  /**
   * Resolve multiple {@code others} against the given {@code directory} sequentially.
   *
   * <p>Sees {@link #resolve} for the differences with {@link URI#resolve}.
   *
   * @throws IllegalArgumentException if others contains {@link URI} query or fragment components.
   */
  public static String resolve(String directory, String other, String... others) {
    String ret = resolve(directory, other, StandardResolveOptions.RESOLVE);
    for (String str : others) {
      ret = resolve(ret, str, StandardResolveOptions.RESOLVE);
    }
    return ret;
  }

  /**
   * Resolve {@code other} against the given {@code directory}.
   *
   * <p>Unlike {@link URI#resolve}, this function includes the last segment of the path component
   * in {@code directory}. For example,
   * {@code FileSystems.resolve("/home", "output")} returns "/home/output".
   *
   * <p>Other rules of {@link URI#resolve} apply the same. For example, ".", ".." are
   * normalized. Sees {@link URI#resolve} and {@link URI#normalize} for details.
   *
   * @throws IllegalArgumentException if other is empty, or is invalid {@link URI},
   * or contains {@link URI} query or fragment components.
   */
  public static String resolve(
      String path, String other, ResolveOptions options) {
    checkArgument(!other.isEmpty(), "Expected other is not empty.");
    try {
      URI pathUri = specToURI(path);
      URI otherUri = specToURI(other);
      return uriToSpec(resolveWithUri(pathUri, otherUri, options));
    } catch (URISyntaxException e) {
      // Try resolve with java.nio.file.Path for non-URI specs.
      return resolveWithPath(path, other, options);
    }
  }

  private static String resolveWithPath(String path, String other, ResolveOptions options) {
    if (isLocalWindowsOSPath(path)) {
      // Replace asterisks in the spec with the placeholder (uuid) before converting to a Path.
      String uuid = UUID.randomUUID().toString();
      String pathAsterisksReplaced = path.replaceAll("\\*", uuid);
      String otherAsterisksReplaced = other.replaceAll("\\*", uuid);

      if (options.equals(StandardResolveOptions.RESOLVE)) {
        return Paths.get(pathAsterisksReplaced)
            .resolve(otherAsterisksReplaced)
            .toString().replaceAll(uuid, "\\*");
      } else if (options.equals(StandardResolveOptions.RESOLVE_SIBLING)) {
        return Paths.get(pathAsterisksReplaced)
            .resolveSibling(otherAsterisksReplaced)
            .toString().replaceAll(uuid, "\\*");
      } else {
        throw new IllegalArgumentException(
            String.format("ResolveOptions: [%s] is not supported.", options));
      }
    } else {
      if (options.equals(StandardResolveOptions.RESOLVE)) {
        return Paths.get(path).resolve(other).toString();
      } else if (options.equals(StandardResolveOptions.RESOLVE_SIBLING)) {
        return Paths.get(path).resolveSibling(other).toString();
      } else {
        throw new IllegalArgumentException(
            String.format("ResolveOptions: [%s] is not supported.", options));
      }
    }
  }

  private static URI resolveWithUri(URI path, URI other, ResolveOptions options)
      throws URISyntaxException {
    checkURIArgument(path);
    checkURIArgument(other);

    URI directory = toDirectory(path);
    if (options.equals(StandardResolveOptions.RESOLVE)) {
      return directory.resolve(other);
    } else if (options.equals(StandardResolveOptions.RESOLVE_SIBLING)) {
      return directory.resolve("..").resolve(other);
    } else {
      throw new IllegalArgumentException(
          String.format("ResolveOptions: [%s] is not supported.", options));
    }
  }

  private static URI toDirectory(URI uri) throws URISyntaxException {
    String path = uri.getPath();
    if (!Strings.isNullOrEmpty(uri.getAuthority()) && Strings.isNullOrEmpty(path)) {
      // Workaround of https://issues.apache.org/jira/browse/BEAM-1174:
      // path needs to be absolute if the authority exists.
      path = URI_DELIMITER;
    } else if (!Strings.isNullOrEmpty(path) && !path.endsWith(URI_DELIMITER)) {
      path = path + URI_DELIMITER;
    }
    return new URI(
        uri.getScheme(),
        uri.getAuthority(),
        path,
        null /* query */,
        null /* fragment */);
  }

  /**
   * Defines the standard resolve options.
   */
  public enum StandardResolveOptions implements ResolveOptions {
    /**
     * Resolves as {@link Path#resolve}.
     */
    RESOLVE,
    /**
     * Resolves as {@link Path#resolveSibling}.
     */
    RESOLVE_SIBLING,
  }

  private interface ResolveOptions {}

  private static void checkURIArgument(URI uri) {
    checkNotNull(uri, "uri");

    checkArgument(
        Strings.isNullOrEmpty(uri.getQuery()),
        String.format("Expected no query in uri, however received: [%s].", uri));
    checkArgument(
        Strings.isNullOrEmpty(uri.getFragment()),
        String.format("Expected no fragment in uri, however received: [%s].", uri));
  }

  /**
   * Converts the user spec string to URI.
   *
   * <p>It uses {@link Paths} to handle non-URI Windows OS paths.
   */
  private static URI specToURI(String spec) throws URISyntaxException {
    // Using the URI constructor in order to escape special characters, such as spaces.
    // URI.create() will throw for illegal characters if it is called directly.
    Matcher matcher = URI_PARSING_PATTERN.matcher(spec);
    checkState(matcher.matches(), "spec: %s doesn't match URI regex.", spec);
    return new URI(
        matcher.group("scheme"),
        matcher.group("authority"),
        matcher.group("path"),
        matcher.group("query"),
        matcher.group("fragment"));
  }

  private static String uriToSpec(URI uri) {
    checkURIArgument(uri);
    String scheme = uri.getScheme();
    return Strings.isNullOrEmpty(scheme)
        ? uri.getSchemeSpecificPart() : String.format("%s:%s", scheme, uri.getSchemeSpecificPart());
  }

  /**
   * Sets the default configuration in workers.
   *
   * <p>It will be used in {@link FileSystemRegistrar FileSystemRegistrars} for all schemes.
   */
  public static void setDefaultConfigInWorkers(PipelineOptions options) {
    defaultConfig = checkNotNull(options, "options");
  }

  @VisibleForTesting
  static boolean isLocalWindowsOSPath(String spec) {
    return getLowerCaseScheme(spec).equals(LocalFileSystemRegistrar.LOCAL_FILE_SCHEME)
        && SystemUtils.IS_OS_WINDOWS;
  }

  @VisibleForTesting
  static String getLowerCaseScheme(String spec) {
    Matcher matcher = URI_SCHEME_PARSING_PATTERN.matcher(spec);
    if (!matcher.matches()) {
      return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
    }
    String scheme = matcher.group("scheme");
    if (Strings.isNullOrEmpty(scheme)) {
      return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
    } else {
      return scheme.toLowerCase();
    }
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(URI uri) {
    checkState(
        defaultConfig != null,
        "Expect the runner have called setDefaultConfigInWorkers().");
    String lowerCaseScheme = (uri.getScheme() != null
        ? uri.getScheme().toLowerCase() : LocalFileSystemRegistrar.LOCAL_FILE_SCHEME);
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
