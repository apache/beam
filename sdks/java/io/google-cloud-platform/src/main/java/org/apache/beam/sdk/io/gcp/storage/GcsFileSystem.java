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
package org.apache.beam.sdk.io.gcp.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for Google Cloud Storage.
 */
class GcsFileSystem extends FileSystem<GcsResourceId> {
  private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystem.class);

  private final GcsOptions options;

  GcsFileSystem(GcsOptions options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected WritableByteChannel create(GcsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return options.getGcsUtil().create(resourceId.getGcsPath(), createOptions.mimeType());
  }

  @Override
  protected ReadableByteChannel open(GcsResourceId resourceId) throws IOException {
    return options.getGcsUtil().open(resourceId.getGcsPath());
  }

  @Override
  protected void rename(
      List<GcsResourceId> srcResourceIds,
      List<GcsResourceId> destResourceIds) throws IOException {
    copy(srcResourceIds, destResourceIds);
    delete(srcResourceIds);
  }

  @Override
  protected void delete(Collection<GcsResourceId> resourceIds) throws IOException {
    options.getGcsUtil().remove(toFilenames(resourceIds));
  }

  @Override
  protected void copy(List<GcsResourceId> srcResourceIds, List<GcsResourceId> destResourceIds)
      throws IOException {
    options.getGcsUtil().copy(toFilenames(srcResourceIds), toFilenames(destResourceIds));
  }

  /**
   * Expands a pattern into {@link MatchResult}.
   *
   * @throws IllegalArgumentException if {@code gcsPattern} does not contain globs.
   */
  @VisibleForTesting
  MatchResult expand(GcsPath gcsPattern) throws IOException {
    String prefix = GcsUtil.getGlobPrefix(gcsPattern.getObject());
    Pattern p = Pattern.compile(GcsUtil.globToRegexp(gcsPattern.getObject()));

    LOG.debug("matching files in bucket {}, prefix {} against pattern {}", gcsPattern.getBucket(),
        prefix, p.toString());

    String pageToken = null;
    List<Metadata> results = new LinkedList<>();
    do {
      Objects objects = options.getGcsUtil().listObjects(gcsPattern.getBucket(), prefix, pageToken);
      if (objects.getItems() == null) {
        break;
      }

      // Filter objects based on the regex.
      for (StorageObject o : objects.getItems()) {
        String name = o.getName();
        // Skip directories, which end with a slash.
        if (p.matcher(name).matches() && !name.endsWith("/")) {
          LOG.debug("Matched object: {}", name);
          results.add(toMetadata(o));
        }
      }
      pageToken = objects.getNextPageToken();
    } while (pageToken != null);
    return MatchResult.create(Status.OK, results.toArray(new Metadata[results.size()]));
  }

  private List<String> toFilenames(Collection<GcsResourceId> resources) {
    return FluentIterable.from(resources)
        .transform(
            new Function<GcsResourceId, String>() {
              @Override
              public String apply(GcsResourceId resource) {
                return resource.getGcsPath().toString();
              }})
        .toList();
  }

  private Metadata toMetadata(StorageObject storageObject) {
    // TODO: Address https://issues.apache.org/jira/browse/BEAM-1494
    // It is incorrect to set IsReadSeekEfficient true for files with content encoding set to gzip.
    Metadata.Builder ret = Metadata.builder()
        .setIsReadSeekEfficient(true)
        .setResourceId(GcsResourceId.fromGcsPath(GcsPath.fromObject(storageObject)));
    BigInteger size = storageObject.getSize();
    if (size != null) {
      ret.setSizeBytes(size.longValue());
    }
    return ret.build();
  }
}
