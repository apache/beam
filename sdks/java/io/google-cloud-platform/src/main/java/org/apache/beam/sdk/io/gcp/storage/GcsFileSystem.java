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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.GcsUtil.StorageObjectOrIOException;
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
    List<GcsPath> gcsPaths = toGcsPaths(specs);

    List<GcsPath> globs = Lists.newArrayList();
    List<GcsPath> nonGlobs = Lists.newArrayList();
    List<Boolean> isGlobBooleans = Lists.newArrayList();

    for (GcsPath path : gcsPaths) {
      if (GcsUtil.isGlob(path)) {
        globs.add(path);
        isGlobBooleans.add(true);
      } else {
        nonGlobs.add(path);
        isGlobBooleans.add(false);
      }
    }

    Iterator<MatchResult> globsMatchResults = matchGlobs(globs).iterator();
    Iterator<MatchResult> nonGlobsMatchResults = matchNonGlobs(nonGlobs).iterator();

    ImmutableList.Builder<MatchResult> ret = ImmutableList.builder();
    for (Boolean isGlob : isGlobBooleans) {
      if (isGlob) {
        checkState(globsMatchResults.hasNext(), "Expect globsMatchResults has next.");
        ret.add(globsMatchResults.next());
      } else {
        checkState(nonGlobsMatchResults.hasNext(), "Expect nonGlobsMatchResults has next.");
        ret.add(nonGlobsMatchResults.next());
      }
    }
    checkState(!globsMatchResults.hasNext(), "Expect no more elements in globsMatchResults.");
    checkState(!nonGlobsMatchResults.hasNext(), "Expect no more elements in nonGlobsMatchResults.");
    return ret.build();
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
  protected GcsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (isDirectory) {
      if (!singleResourceSpec.endsWith("/")) {
        singleResourceSpec += '/';
      }
    } else {
      checkArgument(
          !singleResourceSpec.endsWith("/"),
          "Expected a file path, but [%s], ends with '/'. This is unsupported in GcsFileSystem.",
          singleResourceSpec);
    }
    GcsPath path = GcsPath.fromUri(singleResourceSpec);
    return GcsResourceId.fromGcsPath(path);
  }

  @Override
  protected void copy(List<GcsResourceId> srcResourceIds, List<GcsResourceId> destResourceIds)
      throws IOException {
    options.getGcsUtil().copy(toFilenames(srcResourceIds), toFilenames(destResourceIds));
  }

  @Override
  protected String getScheme() {
    return "gs";
  }

  private List<MatchResult> matchGlobs(List<GcsPath> globs) {
    // TODO: Executes in parallel, address https://issues.apache.org/jira/browse/BEAM-1503.
    return FluentIterable.from(globs)
        .transform(new Function<GcsPath, MatchResult>() {
          @Override
          public MatchResult apply(GcsPath gcsPath) {
            try {
              return expand(gcsPath);
            } catch (IOException e) {
              return MatchResult.create(Status.ERROR, e);
            }
          }})
        .toList();
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
    return MatchResult.create(Status.OK, results);
  }

  /**
   * Returns {@link MatchResult MatchResults} for the given {@link GcsPath GcsPaths}.
   *
   *<p>The number of returned {@link MatchResult MatchResults} equals to the number of given
   * {@link GcsPath GcsPaths}. Each {@link MatchResult} contains one {@link Metadata}.
   */
  @VisibleForTesting
  List<MatchResult> matchNonGlobs(List<GcsPath> gcsPaths) throws IOException {
    List<StorageObjectOrIOException> results = options.getGcsUtil().getObjects(gcsPaths);

    ImmutableList.Builder<MatchResult> ret = ImmutableList.builder();
    for (StorageObjectOrIOException result : results) {
      ret.add(toMatchResult(result));
    }
    return ret.build();
  }

  private MatchResult toMatchResult(StorageObjectOrIOException objectOrException) {
    @Nullable IOException exception = objectOrException.ioException();
    if (exception instanceof FileNotFoundException) {
      return MatchResult.create(Status.NOT_FOUND, exception);
    } else if (exception != null) {
      return MatchResult.create(Status.ERROR, exception);
    } else {
      StorageObject object = objectOrException.storageObject();
      assert object != null; // fix a warning; guaranteed by StorageObjectOrIOException semantics.
      return MatchResult.create(Status.OK, ImmutableList.of(toMetadata(object)));
    }
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

  private List<GcsPath> toGcsPaths(Collection<String> specs) {
    return FluentIterable.from(specs)
        .transform(new Function<String, GcsPath>() {
          @Override
          public GcsPath apply(String spec) {
            return GcsPath.fromUri(spec);
          }})
        .toList();
  }
}
