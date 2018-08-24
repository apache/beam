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
import static com.google.common.base.Verify.verify;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;

/** Clients facing {@link FileSystem} utility. */
@Experimental(Kind.FILESYSTEM)
public class FileSystems {

  public static final String DEFAULT_SCHEME = "file";
  private static final Pattern FILE_SCHEME_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*):/.*");
  private static final Pattern GLOB_PATTERN = Pattern.compile("[*?{}]");

  private static final AtomicReference<Map<String, FileSystem>> SCHEME_TO_FILESYSTEM =
      new AtomicReference<>(ImmutableMap.of(DEFAULT_SCHEME, new LocalFileSystem()));

  /** ******************************** METHODS FOR CLIENT ********************************* */

  /** Checks whether the given spec contains a glob wildcard character. */
  public static boolean hasGlobWildcard(String spec) {
    return GLOB_PATTERN.matcher(spec).find();
  }

  /**
   * This is the entry point to convert user-provided specs to {@link ResourceId ResourceIds}.
   * Callers should use {@link #match} to resolve users specs ambiguities before calling other
   * methods.
   *
   * <p>Implementation handles the following ambiguities of a user-provided spec:
   *
   * <ol>
   *   <li>{@code spec} could be a glob or a uri. {@link #match} should be able to tell and choose
   *       efficient implementations.
   *   <li>The user-provided {@code spec} might refer to files or directories. It is common that
   *       users that wish to indicate a directory will omit the trailing path delimiter, such as
   *       {@code "/tmp/dir"} in Linux. The {@link FileSystem} should be able to recognize a
   *       directory with the trailing path delimiter omitted, but should always return a correct
   *       {@link ResourceId} (e.g., {@code "/tmp/dir/"} inside the returned {@link MatchResult}.
   * </ol>
   *
   * <p>All {@link FileSystem} implementations should support glob in the final hierarchical path
   * component of {@link ResourceId}. This allows SDK libraries to construct file system agnostic
   * spec. {@link FileSystem FileSystems} can support additional patterns for user-provided specs.
   *
   * <p>In case the spec schemes don't match any known {@link FileSystem} implementations,
   * FileSystems will attempt to use {@link LocalFileSystem} to resolve a path.
   *
   * <p>Specs that do not match any resources are treated according to {@link
   * EmptyMatchTreatment#DISALLOW}.
   *
   * @return {@code List<MatchResult>} in the same order of the input specs.
   * @throws IllegalArgumentException if specs are invalid -- empty or have different schemes.
   * @throws IOException if all specs failed to match due to issues like: network connection,
   *     authorization. Exception for individual spec is deferred until callers retrieve metadata
   *     with {@link MatchResult#metadata()}.
   */
  public static List<MatchResult> match(List<String> specs) throws IOException {
    return getFileSystemInternal(getOnlyScheme(specs)).match(specs);
  }

  /** Like {@link #match(List)}, but with a configurable {@link EmptyMatchTreatment}. */
  public static List<MatchResult> match(List<String> specs, EmptyMatchTreatment emptyMatchTreatment)
      throws IOException {
    List<MatchResult> matches = getFileSystemInternal(getOnlyScheme(specs)).match(specs);
    List<MatchResult> res = Lists.newArrayListWithExpectedSize(matches.size());
    for (int i = 0; i < matches.size(); i++) {
      res.add(maybeAdjustEmptyMatchResult(specs.get(i), matches.get(i), emptyMatchTreatment));
    }
    return res;
  }

  /**
   * Like {@link #match(List)}, but for a single resource specification.
   *
   * <p>The function {@link #match(List)} is preferred when matching multiple patterns, as it allows
   * for bulk API calls to remote filesystems.
   */
  public static MatchResult match(String spec) throws IOException {
    List<MatchResult> matches = match(Collections.singletonList(spec));
    verify(
        matches.size() == 1,
        "FileSystem implementation for %s did not return exactly one MatchResult: %s",
        spec,
        matches);
    return matches.get(0);
  }

  /** Like {@link #match(String)}, but with a configurable {@link EmptyMatchTreatment}. */
  public static MatchResult match(String spec, EmptyMatchTreatment emptyMatchTreatment)
      throws IOException {
    MatchResult res = match(spec);
    return maybeAdjustEmptyMatchResult(spec, res, emptyMatchTreatment);
  }

  private static MatchResult maybeAdjustEmptyMatchResult(
      String spec, MatchResult res, EmptyMatchTreatment emptyMatchTreatment) throws IOException {
    if (res.status() == Status.NOT_FOUND
        || (res.status() == Status.OK && res.metadata().isEmpty())) {
      boolean notFoundAllowed =
          emptyMatchTreatment == EmptyMatchTreatment.ALLOW
              || (hasGlobWildcard(spec)
                  && emptyMatchTreatment == EmptyMatchTreatment.ALLOW_IF_WILDCARD);
      return notFoundAllowed
          ? MatchResult.create(Status.OK, Collections.emptyList())
          : MatchResult.create(
              Status.NOT_FOUND, new FileNotFoundException("No files matched spec: " + spec));
    }
    return res;
  }

  /**
   * Returns the {@link Metadata} for a single file resource. Expects a resource specification
   * {@code spec} that matches a single result.
   *
   * @param spec a resource specification that matches exactly one result.
   * @return the {@link Metadata} for the specified resource.
   * @throws FileNotFoundException if the file resource is not found.
   * @throws IOException in the event of an error in the inner call to {@link #match}, or if the
   *     given spec does not match exactly 1 result.
   */
  public static Metadata matchSingleFileSpec(String spec) throws IOException {
    List<MatchResult> matches = FileSystems.match(Collections.singletonList(spec));
    MatchResult matchResult = Iterables.getOnlyElement(matches);
    if (matchResult.status() == Status.NOT_FOUND) {
      throw new FileNotFoundException(String.format("File spec %s not found", spec));
    } else if (matchResult.status() != Status.OK) {
      throw new IOException(
          String.format("Error matching file spec %s: status %s", spec, matchResult.status()));
    } else {
      List<Metadata> metadata = matchResult.metadata();
      if (metadata.size() != 1) {
        throw new IOException(
            String.format(
                "Expecting spec %s to match exactly one file, but matched %s: %s",
                spec, metadata.size(), metadata));
      }
      return metadata.get(0);
    }
  }

  /**
   * Returns {@link MatchResult MatchResults} for the given {@link ResourceId resourceIds}.
   *
   * @param resourceIds {@link ResourceId resourceIds} that might be derived from {@link #match},
   *     {@link ResourceId#resolve}, or {@link ResourceId#getCurrentDirectory()}.
   * @throws IOException if all {@code resourceIds} failed to match due to issues like: network
   *     connection, authorization. Exception for individual {@link ResourceId} need to be deferred
   *     until callers retrieve metadata with {@link MatchResult#metadata()}.
   */
  public static List<MatchResult> matchResources(List<ResourceId> resourceIds) throws IOException {
    return match(FluentIterable.from(resourceIds).transform(ResourceId::toString).toList());
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
   * <p>If seeking is supported, then this returns a {@link java.nio.channels.SeekableByteChannel}.
   *
   * @param resourceId the reference of the file-like resource to open
   */
  public static ReadableByteChannel open(ResourceId resourceId) throws IOException {
    return getFileSystemInternal(resourceId.getScheme()).open(resourceId);
  }

  /**
   * Copies a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources. Destination
   * resources will be created recursively.
   *
   * <p>{@code srcResourceIds} and {@code destResourceIds} must have the same scheme.
   *
   * <p>It doesn't support copying globs.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   */
  public static void copy(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds, MoveOptions... moveOptions)
      throws IOException {
    validateSrcDestLists(srcResourceIds, destResourceIds);
    if (srcResourceIds.isEmpty()) {
      // Short-circuit.
      return;
    }

    List<ResourceId> srcToCopy = srcResourceIds;
    List<ResourceId> destToCopy = destResourceIds;
    if (Sets.newHashSet(moveOptions)
        .contains(MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      KV<List<ResourceId>, List<ResourceId>> existings =
          filterMissingFiles(srcResourceIds, destResourceIds);
      srcToCopy = existings.getKey();
      destToCopy = existings.getValue();
    }
    if (srcToCopy.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcToCopy.iterator().next().getScheme()).copy(srcToCopy, destToCopy);
  }

  /**
   * Renames a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources. Destination
   * resources will be created recursively.
   *
   * <p>{@code srcResourceIds} and {@code destResourceIds} must have the same scheme.
   *
   * <p>It doesn't support renaming globs.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   */
  public static void rename(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds, MoveOptions... moveOptions)
      throws IOException {
    validateSrcDestLists(srcResourceIds, destResourceIds);
    if (srcResourceIds.isEmpty()) {
      // Short-circuit.
      return;
    }

    List<ResourceId> srcToRename = srcResourceIds;
    List<ResourceId> destToRename = destResourceIds;
    if (Sets.newHashSet(moveOptions)
        .contains(MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      KV<List<ResourceId>, List<ResourceId>> existings =
          filterMissingFiles(srcResourceIds, destResourceIds);
      srcToRename = existings.getKey();
      destToRename = existings.getValue();
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
   * <p>{@code resourceIds} must have the same scheme.
   *
   * @param resourceIds the references of the resources to delete.
   */
  public static void delete(Collection<ResourceId> resourceIds, MoveOptions... moveOptions)
      throws IOException {
    if (resourceIds.isEmpty()) {
      // Short-circuit.
      return;
    }

    Collection<ResourceId> resourceIdsToDelete;
    if (Sets.newHashSet(moveOptions)
        .contains(MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      resourceIdsToDelete =
          FluentIterable.from(matchResources(Lists.newArrayList(resourceIds)))
              .filter(matchResult -> !matchResult.status().equals(Status.NOT_FOUND))
              .transformAndConcat(
                  new Function<MatchResult, Iterable<Metadata>>() {
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
                    }
                  })
              .transform(
                  new Function<Metadata, ResourceId>() {
                    @Nonnull
                    @Override
                    public ResourceId apply(@Nonnull Metadata input) {
                      return input.resourceId();
                    }
                  })
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
    validateSrcDestLists(srcResourceIds, destResourceIds);
    if (srcResourceIds.isEmpty()) {
      // Short-circuit.
      return KV.of(Collections.<ResourceId>emptyList(), Collections.<ResourceId>emptyList());
    }

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

  private static void validateSrcDestLists(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds) {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source resource ids %s must equal number of destination resource ids %s",
        srcResourceIds.size(),
        destResourceIds.size());

    if (srcResourceIds.isEmpty()) {
      // nothing more to validate.
      return;
    }

    Set<String> schemes =
        FluentIterable.from(srcResourceIds)
            .append(destResourceIds)
            .transform(ResourceId::getScheme)
            .toSet();
    checkArgument(
        schemes.size() == 1,
        String.format(
            "Expect srcResourceIds and destResourceIds have the same scheme, but received %s.",
            Joiner.on(", ").join(schemes)));
  }

  private static String getOnlyScheme(List<String> specs) {
    checkArgument(!specs.isEmpty(), "Expect specs are not empty.");
    Set<String> schemes = FluentIterable.from(specs).transform(FileSystems::parseScheme).toSet();
    return Iterables.getOnlyElement(schemes);
  }

  private static String parseScheme(String spec) {
    // The spec is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, which is so circumscribed as to be
    // very easy to extract with a regex.
    Matcher matcher = FILE_SCHEME_PATTERN.matcher(spec);

    if (!matcher.matches()) {
      return DEFAULT_SCHEME;
    } else {
      return matcher.group("scheme").toLowerCase();
    }
  }

  /** Internal method to get {@link FileSystem} for {@code scheme}. */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(String scheme) {
    String lowerCaseScheme = scheme.toLowerCase();
    Map<String, FileSystem> schemeToFileSystem = SCHEME_TO_FILESYSTEM.get();
    FileSystem rval = schemeToFileSystem.get(lowerCaseScheme);
    if (rval == null) {
      throw new IllegalArgumentException("No filesystem found for scheme " + scheme);
    }
    return rval;
  }

  /** ******************************** METHODS FOR REGISTRATION ********************************* */

  /**
   * Sets the default configuration in workers.
   *
   * <p>It will be used in {@link FileSystemRegistrar FileSystemRegistrars} for all schemes.
   *
   * <p>This is expected only to be used by runners after {@code Pipeline.run}, or in tests.
   */
  @Internal
  public static void setDefaultPipelineOptions(PipelineOptions options) {
    checkNotNull(options, "options");
    Set<FileSystemRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader())));

    SCHEME_TO_FILESYSTEM.set(verifySchemesAreUnique(options, registrars));
  }

  @VisibleForTesting
  static Map<String, FileSystem> verifySchemesAreUnique(
      PipelineOptions options, Set<FileSystemRegistrar> registrars) {
    Multimap<String, FileSystem> fileSystemsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (FileSystemRegistrar registrar : registrars) {
      for (FileSystem fileSystem : registrar.fromOptions(options)) {
        fileSystemsBySchemes.put(fileSystem.getScheme(), fileSystem);
      }
    }
    for (Entry<String, Collection<FileSystem>> entry : fileSystemsBySchemes.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        String conflictingFileSystems =
            Joiner.on(", ")
                .join(
                    FluentIterable.from(entry.getValue())
                        .transform(input -> input.getClass().getName())
                        .toSortedList(Ordering.natural()));
        throw new IllegalStateException(
            String.format(
                "Scheme: [%s] has conflicting filesystems: [%s]",
                entry.getKey(), conflictingFileSystems));
      }
    }

    ImmutableMap.Builder<String, FileSystem> schemeToFileSystem = ImmutableMap.builder();
    for (Entry<String, FileSystem> entry : fileSystemsBySchemes.entries()) {
      schemeToFileSystem.put(entry.getKey(), entry.getValue());
    }
    return schemeToFileSystem.build();
  }

  /**
   * Returns a new {@link ResourceId} that represents the named resource of a type corresponding to
   * the resource type.
   *
   * <p>The supplied {@code singleResourceSpec} is expected to be in a proper format, including any
   * necessary escaping, for the underlying {@link FileSystem}.
   *
   * <p>This function may throw an {@link IllegalArgumentException} if given an invalid argument,
   * such as when the specified {@code singleResourceSpec} is not a valid resource name.
   */
  public static ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    return getFileSystemInternal(parseScheme(singleResourceSpec))
        .matchNewResource(singleResourceSpec, isDirectory);
  }
}
