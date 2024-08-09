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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Lineage;

/**
 * File system interface in Beam.
 *
 * <p>It defines APIs for writing file systems agnostic code.
 *
 * <p>All methods are protected, and they are for file system providers to implement. Clients should
 * use the {@link FileSystems} utility.
 */
public abstract class FileSystem<ResourceIdT extends ResourceId> {
  /**
   * This is the entry point to convert user-provided specs to {@link ResourceIdT ResourceIds}.
   * Callers should use {@link #match} to resolve users specs ambiguities before calling other
   * methods.
   *
   * <p>Implementation should handle the following ambiguities of a user-provided spec:
   *
   * <ol>
   *   <li>{@code spec} could be a glob or a uri. {@link #match} should be able to tell and choose
   *       efficient implementations.
   *   <li>The user-provided {@code spec} might refer to files or directories. It is common that
   *       users that wish to indicate a directory will omit the trailing {@code /}, such as in a
   *       spec of {@code "/tmp/dir"}. The {@link FileSystem} should be able to recognize a
   *       directory with the trailing {@code /} omitted, but should always return a correct {@link
   *       ResourceIdT} (e.g., {@code "/tmp/dir/"} inside the returned {@link MatchResult}.
   * </ol>
   *
   * <p>All {@link FileSystem} implementations should support glob in the final hierarchical path
   * component of {@link ResourceIdT}. This allows SDK libraries to construct file system agnostic
   * spec. {@link FileSystem FileSystems} can support additional patterns for user-provided specs.
   *
   * @return {@code List<MatchResult>} in the same order of the input specs.
   * @throws IllegalArgumentException if specs are invalid.
   * @throws IOException if all specs failed to match due to issues like: network connection,
   *     authorization. Exception for individual spec need to be deferred until callers retrieve
   *     metadata with {@link MatchResult#metadata()}.
   */
  protected abstract List<MatchResult> match(List<String> specs) throws IOException;

  /**
   * Returns a write channel for the given {@link ResourceIdT}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * @param resourceId the reference of the file-like resource to create
   * @param createOptions the configuration of the create operation
   */
  protected abstract WritableByteChannel create(ResourceIdT resourceId, CreateOptions createOptions)
      throws IOException;

  /**
   * Returns a read channel for the given {@link ResourceIdT}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a {@link java.nio.channels.SeekableByteChannel}.
   *
   * @param resourceId the reference of the file-like resource to open
   */
  protected abstract ReadableByteChannel open(ResourceIdT resourceId) throws IOException;

  /**
   * Copies a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources. Destination
   * resources will be created recursively.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   * @throws FileNotFoundException if the source resources are missing. When copy throws, each
   *     resource might or might not be copied. In such scenarios, callers can use {@code match()}
   *     to determine the state of the resources.
   */
  protected abstract void copy(List<ResourceIdT> srcResourceIds, List<ResourceIdT> destResourceIds)
      throws IOException;

  /**
   * Renames a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources. Destination
   * resources will be created recursively.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   * @param moveOptions move options specifying handling of error conditions
   * @throws UnsupportedOperationException if move options are specified and not supported by the
   *     FileSystem
   * @throws FileNotFoundException if the source resources are missing. When rename throws, the
   *     state of the resources is unknown but safe: for every (source, destination) pair of
   *     resources, the following are possible: a) source exists, b) destination exists, c) source
   *     and destination both exist. Thus no data is lost, however, duplicated resource are
   *     possible. In such scenarios, callers can use {@code match()} to determine the state of the
   *     resource.
   */
  protected abstract void rename(
      List<ResourceIdT> srcResourceIds,
      List<ResourceIdT> destResourceIds,
      MoveOptions... moveOptions)
      throws IOException;

  /**
   * Deletes a collection of resources.
   *
   * @param resourceIds the references of the resources to delete.
   * @throws FileNotFoundException if resources are missing. When delete throws, each resource might
   *     or might not be deleted. In such scenarios, callers can use {@code match()} to determine
   *     the state of the resources.
   */
  protected abstract void delete(Collection<ResourceIdT> resourceIds) throws IOException;

  /**
   * Returns a new {@link ResourceId} for this filesystem that represents the named resource. The
   * user supplies both the resource spec and whether it is a directory.
   *
   * <p>The supplied {@code singleResourceSpec} is expected to be in a proper format, including any
   * necessary escaping, for this {@link FileSystem}.
   *
   * <p>This function may throw an {@link IllegalArgumentException} if given an invalid argument,
   * such as when the specified {@code singleResourceSpec} is not a valid resource name.
   */
  protected abstract ResourceIdT matchNewResource(String singleResourceSpec, boolean isDirectory);

  /**
   * Get the URI scheme which defines the namespace of the {@link FileSystem}.
   *
   * @see <a href="https://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
   */
  protected abstract String getScheme();

  /**
   * Report {@link Lineage} metrics for resource id.
   *
   * <p>Unless override by FileSystem implementations, default to no-op.
   */
  protected void reportLineage(ResourceIdT unusedId, Lineage unusedLineage) {}
}
