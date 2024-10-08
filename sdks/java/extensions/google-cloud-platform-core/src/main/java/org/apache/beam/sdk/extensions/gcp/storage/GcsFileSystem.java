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
package org.apache.beam.sdk.extensions.gcp.storage;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for Google Cloud Storage.
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant updates to this I/O connector, please consider involving corresponding code
 * reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/extensions/google-cloud-platform-core/OWNERS">
 * here</a>.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class GcsFileSystem extends FileSystem<GcsResourceId> {
  private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystem.class);

  private final GcsOptions options;

  /** Number of copy operations performed. */
  private Counter numCopies;

  /** Number of renames operations performed. */
  private Counter numRenames;

  /** Time spent performing copies. */
  private Counter copyTimeMsec;

  /** Time spent performing renames. */
  private Counter renameTimeMsec;

  GcsFileSystem(GcsOptions options) {
    this.options = checkNotNull(options, "options");
    if (options.getGcsPerformanceMetrics()) {
      numCopies = Metrics.counter(GcsFileSystem.class, "num_copies");
      copyTimeMsec = Metrics.counter(GcsFileSystem.class, "copy_time_msec");
      numRenames = Metrics.counter(GcsFileSystem.class, "num_renames");
      renameTimeMsec = Metrics.counter(GcsFileSystem.class, "rename_time_msec");
    }
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<GcsPath> gcsPaths = toGcsPaths(specs);

    List<GcsPath> globs = Lists.newArrayList();
    List<GcsPath> nonGlobs = Lists.newArrayList();
    List<Boolean> isGlobBooleans = Lists.newArrayList();

    for (GcsPath path : gcsPaths) {
      if (GcsUtil.isWildcard(path)) {
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
        checkState(globsMatchResults.hasNext(), "Expect globsMatchResults has next: %s", globs);
        ret.add(globsMatchResults.next());
      } else {
        checkState(
            nonGlobsMatchResults.hasNext(), "Expect nonGlobsMatchResults has next: %s", nonGlobs);
        ret.add(nonGlobsMatchResults.next());
      }
    }
    checkState(
        !globsMatchResults.hasNext(),
        "Internal error encountered in GcsFilesystem: expected no more elements in globsMatchResults.");
    checkState(
        !nonGlobsMatchResults.hasNext(),
        "Internal error encountered in GcsFilesystem: expected no more elements in globsMatchResults.");
    return ret.build();
  }

  @Override
  protected WritableByteChannel create(GcsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    GcsUtil.CreateOptions.Builder builder =
        GcsUtil.CreateOptions.builder()
            .setContentType(createOptions.mimeType())
            .setExpectFileToNotExist(createOptions.expectFileToNotExist());
    if (createOptions instanceof GcsCreateOptions) {
      builder =
          builder.setUploadBufferSizeBytes(
              ((GcsCreateOptions) createOptions).gcsUploadBufferSizeBytes());
    }
    return options.getGcsUtil().create(resourceId.getGcsPath(), builder.build());
  }

  @Override
  protected ReadableByteChannel open(GcsResourceId resourceId) throws IOException {
    return options.getGcsUtil().open(resourceId.getGcsPath());
  }

  @Override
  protected void rename(
      List<GcsResourceId> srcResourceIds,
      List<GcsResourceId> destResourceIds,
      MoveOptions... moveOptions)
      throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    options
        .getGcsUtil()
        .rename(toFilenames(srcResourceIds), toFilenames(destResourceIds), moveOptions);
    stopwatch.stop();
    if (options.getGcsPerformanceMetrics()) {
      numRenames.inc(srcResourceIds.size());
      renameTimeMsec.inc(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void delete(Collection<GcsResourceId> resourceIds) throws IOException {
    options.getGcsUtil().remove(toFilenames(resourceIds));
  }

  @Override
  protected GcsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (isDirectory) {
      if (!singleResourceSpec.endsWith("/")) {
        singleResourceSpec += "/";
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
    Stopwatch stopwatch = Stopwatch.createStarted();
    options.getGcsUtil().copy(toFilenames(srcResourceIds), toFilenames(destResourceIds));
    stopwatch.stop();
    if (options.getGcsPerformanceMetrics()) {
      numCopies.inc(srcResourceIds.size());
      copyTimeMsec.inc(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected String getScheme() {
    return "gs";
  }

  @Override
  protected void reportLineage(GcsResourceId resourceId, Lineage lineage) {
    reportLineage(resourceId, lineage, LineageLevel.FILE);
  }

  @Override
  protected void reportLineage(GcsResourceId resourceId, Lineage lineage, LineageLevel level) {
    GcsPath path = resourceId.getGcsPath();
    if (!path.getBucket().isEmpty()) {
      ImmutableList.Builder<String> segments =
          ImmutableList.<String>builder().add(path.getBucket());
      if (level != LineageLevel.TOP_LEVEL && !path.getObject().isEmpty()) {
        segments.add(path.getObject());
      }
      lineage.add("gcs", segments.build());
    } else {
      LOG.warn("Report Lineage on relative path {} is unsupported", path.getObject());
    }
  }

  private List<MatchResult> matchGlobs(List<GcsPath> globs) {
    // TODO: Executes in parallel, address https://issues.apache.org/jira/browse/BEAM-1503.
    return FluentIterable.from(globs)
        .transform(
            gcsPath -> {
              try {
                return expand(gcsPath);
              } catch (IOException e) {
                return MatchResult.create(Status.ERROR, e);
              }
            })
        .toList();
  }

  /**
   * Expands a pattern into {@link MatchResult}.
   *
   * @throws IllegalArgumentException if {@code gcsPattern} does not contain globs.
   */
  @VisibleForTesting
  MatchResult expand(GcsPath gcsPattern) throws IOException {
    String prefix = GcsUtil.getNonWildcardPrefix(gcsPattern.getObject());
    Pattern p = Pattern.compile(wildcardToRegexp(gcsPattern.getObject()));

    LOG.debug(
        "matching files in bucket {}, prefix {} against pattern {}",
        gcsPattern.getBucket(),
        prefix,
        p.toString());

    String pageToken = null;
    List<Metadata> results = new ArrayList<>();
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
   * <p>The number of returned {@link MatchResult MatchResults} equals to the number of given {@link
   * GcsPath GcsPaths}. Each {@link MatchResult} contains one {@link Metadata}.
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
    Metadata.Builder ret =
        Metadata.builder()
            .setIsReadSeekEfficient(true)
            .setResourceId(GcsResourceId.fromGcsPath(GcsPath.fromObject(storageObject)));
    if (storageObject.getMd5Hash() != null) {
      ret.setChecksum(storageObject.getMd5Hash());
    }
    BigInteger size = firstNonNull(storageObject.getSize(), BigInteger.ZERO);
    ret.setSizeBytes(size.longValue());
    DateTime lastModified = firstNonNull(storageObject.getUpdated(), new DateTime(0L));
    ret.setLastModifiedMillis(lastModified.getValue());
    return ret.build();
  }

  private List<String> toFilenames(Collection<GcsResourceId> resources) {
    return FluentIterable.from(resources)
        .transform(resource -> resource.getGcsPath().toString())
        .toList();
  }

  private List<GcsPath> toGcsPaths(Collection<String> specs) {
    return FluentIterable.from(specs).transform(GcsPath::fromUri).toList();
  }
}
