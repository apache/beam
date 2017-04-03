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
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  public static final String DEFAULT_SCHEME = "default";
  private static final Pattern URI_SCHEME_PATTERN = Pattern.compile(
      "(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  private static final Map<String, FileSystemRegistrar> SCHEME_TO_REGISTRAR =
      new ConcurrentHashMap<>();

  private static PipelineOptions defaultConfig;

  static {
    loadFileSystemRegistrars();
  }

  /********************************** METHODS FOR CLIENT **********************************/

  /**
   * This is the entry point to convert user-provided specs to {@link ResourceId ResourceIds}.
   * Callers should use {@link #match} to resolve users specs ambiguities before
   * calling other methods.
   *
   * <p>Implementation handles the following ambiguities of a user-provided spec:
   * <ol>
   * <li>{@code spec} could be a glob or a uri. {@link #match} should be able to tell and
   * choose efficient implementations.
   * <li>The user-provided {@code spec} might refer to files or directories. It is common that
   * users that wish to indicate a directory will omit the trailing path delimiter, such as
   * {@code "/tmp/dir"} in Linux. The {@link FileSystem} should be able to recognize a directory
   * with the trailing path delimiter omitted, but should always return a correct {@link ResourceId}
   * (e.g., {@code "/tmp/dir/"} inside the returned {@link MatchResult}.
   * </ol>
   *
   * <p>All {@link FileSystem} implementations should support glob in the final hierarchical path
   * component of {@link ResourceId}. This allows SDK libraries to construct file system agnostic
   * spec. {@link FileSystem FileSystems} can support additional patterns for user-provided specs.
   *
   * @return {@code List<MatchResult>} in the same order of the input specs.
   *
   * @throws IllegalArgumentException if specs are invalid -- empty or have different schemes.
   * @throws IOException if all specs failed to match due to issues like:
   * network connection, authorization.
   * Exception for individual spec is deferred until callers retrieve
   * metadata with {@link MatchResult#metadata()}.
   */
  public static List<MatchResult> match(List<String> specs) throws IOException {
    return getFileSystemInternal(getOnlyScheme(specs)).match(specs);
  }

  /**
   * Returns {@link MatchResult MatchResults} for the given {@link ResourceId resourceIds}.
   *
   * @param resourceIds {@link ResourceId resourceIds} that might be derived from {@link #match},
   * {@link ResourceId#resolve}, or {@link ResourceId#getCurrentDirectory()}.
   *
   * @throws IOException if all {@code resourceIds} failed to match due to issues like:
   * network connection, authorization.
   * Exception for individual {@link ResourceId} need to be deferred until callers retrieve
   * metadata with {@link MatchResult#metadata()}.
   */
  public static List<MatchResult> matchResources(List<ResourceId> resourceIds) throws IOException {
    return match(FluentIterable
        .from(resourceIds)
        .transform(new Function<ResourceId, String>() {
          @Override
          public String apply(@Nonnull ResourceId resourceId) {
          return resourceId.toString();
          }})
        .toList());
  }

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
    return getFileSystemInternal(resourceId.getScheme()).create(resourceId, createOptions);
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
    return getFileSystemInternal(resourceId.getScheme()).open(resourceId);
  }

  /**
   * Copies a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources.
   * Destination resources will be created recursively.
   *
   * <p>{@code srcResourceIds} and {@code destResourceIds} must have the same scheme.
   *
   * <p>It doesn't support copying globs.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   */
  public static void copy(
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds,
      MoveOptions... moveOptions) throws IOException {
    validateOnlyScheme(srcResourceIds, destResourceIds);

    List<ResourceId> srcToCopy;
    List<ResourceId> destToCopy;
    if (Sets.newHashSet(moveOptions).contains(
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      KV<List<ResourceId>, List<ResourceId>> existings =
          filterMissingFiles(srcResourceIds, destResourceIds);
      srcToCopy = existings.getKey();
      destToCopy = existings.getValue();
    } else {
      srcToCopy = srcResourceIds;
      destToCopy = destResourceIds;
    }
    if (srcToCopy.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcToCopy.iterator().next().getScheme())
        .copy(srcToCopy, destToCopy);
  }

  /**
   * Renames a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources.
   * Destination resources will be created recursively.
   *
   * <p>{@code srcResourceIds} and {@code destResourceIds} must have the same scheme.
   *
   * <p>It doesn't support renaming globs.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   */
  public static void rename(
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds,
      MoveOptions... moveOptions) throws IOException {
    validateOnlyScheme(srcResourceIds, destResourceIds);
    List<ResourceId> srcToRename;
    List<ResourceId> destToRename;

    if (Sets.newHashSet(moveOptions).contains(
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      KV<List<ResourceId>, List<ResourceId>> existings =
          filterMissingFiles(srcResourceIds, destResourceIds);
      srcToRename = existings.getKey();
      destToRename = existings.getValue();
    } else {
      srcToRename = srcResourceIds;
      destToRename = destResourceIds;
    }
    if (srcToRename.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcToRename.iterator().next().getScheme())
        .rename(srcToRename, destToRename);
  }

  /**
   * Deletes a collection of resources.
   *
   * <p>It is allowed but not recommended to delete directories recursively.
   * Callers depends on {@link FileSystems} and uses {@code DeleteOptions}.
   *
   * <p>{@code resourceIds} must have the same scheme.
   *
   * @param resourceIds the references of the resources to delete.
   */
  public static void delete(
      Collection<ResourceId> resourceIds, MoveOptions... moveOptions) throws IOException {
    Collection<ResourceId> resourceIdsToDelete;
    if (Sets.newHashSet(moveOptions).contains(
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      resourceIdsToDelete = FluentIterable
          .from(matchResources(Lists.newArrayList(resourceIds)))
          .filter(new Predicate<MatchResult>() {
            @Override
            public boolean apply(@Nonnull MatchResult matchResult) {
              return !matchResult.status().equals(MatchResult.Status.NOT_FOUND);
            }})
          .transformAndConcat(new Function<MatchResult, Iterable<Metadata>>() {
            @Nonnull
            @Override
            public Iterable<Metadata> apply(@Nonnull MatchResult input) {
              try {
                return Lists.newArrayList(input.metadata());
              } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Failed to get metadata from MatchResult: %s.", input),
                    e);
              }
            }})
          .transform(new Function<Metadata, ResourceId>() {
            @Nonnull
            @Override
            public ResourceId apply(@Nonnull Metadata input) {
              return input.resourceId();
            }})
          .toList();
    } else {
      resourceIdsToDelete = resourceIds;
    }
    if (resourceIdsToDelete.isEmpty()) {
      return;
    }
    getFileSystemInternal(resourceIdsToDelete.iterator().next().getScheme())
        .delete(resourceIdsToDelete);
  }

  private static KV<List<ResourceId>, List<ResourceId>> filterMissingFiles(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds) throws IOException {
    List<ResourceId> srcToHandle = new ArrayList<>();
    List<ResourceId> destToHandle = new ArrayList<>();

    List<MatchResult> matchResults = matchResources(srcResourceIds);
    for (int i = 0; i < matchResults.size(); ++i) {
      if (!matchResults.get(i).status().equals(Status.NOT_FOUND)) {
        srcToHandle.add(srcResourceIds.get(i));
        destToHandle.add(destResourceIds.get(i));
      }
    }
    return KV.of(srcToHandle, destToHandle);
  }

  private static void validateOnlyScheme(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds) {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source resource ids %s must equal number of destination resource ids %s",
        srcResourceIds.size(),
        destResourceIds.size());
    Set<String> schemes = FluentIterable.from(srcResourceIds)
        .append(destResourceIds)
        .transform(new Function<ResourceId, String>() {
          @Override
          public String apply(@Nonnull ResourceId resourceId) {
            return resourceId.getScheme();
          }})
        .toSet();
    checkArgument(
        schemes.size() == 1,
        String.format(
            "Expect srcResourceIds and destResourceIds have the same scheme, but received %s.",
            Joiner.on(", ").join(schemes)));
  }

  private static String getOnlyScheme(List<String> specs) {
    checkArgument(!specs.isEmpty(), "Expect specs are not empty.");
    Set<String> schemes = FluentIterable.from(specs)
        .transform(new Function<String, String>() {
          @Override
          public String apply(String spec) {
            return parseScheme(spec);
          }})
        .toSet();
    return Iterables.getOnlyElement(schemes);
  }

  private static String parseScheme(String spec) {
    // The spec is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, which is so circumscribed as to be
    // very easy to extract with a regex.
    Matcher matcher = URI_SCHEME_PATTERN.matcher(spec);

    if (!matcher.matches()) {
      return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
    } else {
      return matcher.group("scheme").toLowerCase();
    }
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(String scheme) {
    return getRegistrarInternal(scheme.toLowerCase()).fromOptions(defaultConfig);
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
