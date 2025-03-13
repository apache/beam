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
package org.apache.beam.sdk.io.aws2.s3;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.ObjectStreamException;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An identifier which represents a S3Object resource. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class S3ResourceId implements ResourceId {

  private static final long serialVersionUID = 859142691936154236L;

  static final String DEFAULT_SCHEME = "s3";

  private static final Pattern S3_URI =
      Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<KEY>.*))?");

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  private final String bucket;
  private final String key;
  private final @Nullable Long size;
  private final @Nullable Date lastModified;
  private final String scheme;

  private S3ResourceId(
      String scheme, String bucket, String key, @Nullable Long size, @Nullable Date lastModified) {
    checkArgument(!Strings.isNullOrEmpty(scheme), "scheme");
    checkArgument(!Strings.isNullOrEmpty(bucket), "bucket");
    checkArgument(!bucket.contains("/"), "bucket must not contain '/': [%s]", bucket);
    this.scheme = scheme;
    this.bucket = bucket;
    this.key = checkNotNull(key, "key");
    this.size = size;
    this.lastModified = lastModified;
  }

  private Object readResolve() throws ObjectStreamException {
    if (scheme == null) {
      return new S3ResourceId(DEFAULT_SCHEME, bucket, key, size, lastModified);
    }
    return this;
  }

  static S3ResourceId fromComponents(String scheme, String bucket, String key) {
    if (!key.startsWith("/")) {
      key = "/" + key;
    }
    return new S3ResourceId(scheme, bucket, key, null, null);
  }

  static S3ResourceId fromUri(String uri) {
    Matcher m = S3_URI.matcher(uri);
    checkArgument(m.matches(), "Invalid S3 URI: [%s]", uri);
    String scheme = m.group("SCHEME");
    String bucket = m.group("BUCKET");
    String key = Strings.nullToEmpty(m.group("KEY"));
    if (!key.startsWith("/")) {
      key = "/" + key;
    }
    return fromComponents(scheme, bucket, key);
  }

  String getBucket() {
    return bucket;
  }

  String getKey() {
    // Skip leading slash
    return key.substring(1);
  }

  Optional<Long> getSize() {
    return Optional.fromNullable(size);
  }

  S3ResourceId withSize(long size) {
    return new S3ResourceId(scheme, bucket, key, size, lastModified);
  }

  Optional<Date> getLastModified() {
    return Optional.fromNullable(lastModified);
  }

  S3ResourceId withLastModified(Date lastModified) {
    return new S3ResourceId(scheme, bucket, key, size, lastModified);
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(isDirectory(), "Expected this resource to be a directory, but was [%s]", toString());

    if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY) {
      if ("..".equals(other)) {
        if ("/".equals(key)) {
          return this;
        }
        int parentStopsAt = key.substring(0, key.length() - 1).lastIndexOf('/');
        return fromComponents(scheme, bucket, key.substring(0, parentStopsAt + 1));
      }

      if ("".equals(other)) {
        return this;
      }

      if (!other.endsWith("/")) {
        other += "/";
      }
      if (S3_URI.matcher(other).matches()) {
        return resolveFromUri(other);
      }
      return fromComponents(scheme, bucket, key + other);
    }

    if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_FILE) {
      checkArgument(
          !other.endsWith("/"), "Cannot resolve a file with a directory path: [%s]", other);
      checkArgument(!"..".equals(other), "Cannot resolve parent as file: [%s]", other);
      if (S3_URI.matcher(other).matches()) {
        return resolveFromUri(other);
      }
      return fromComponents(scheme, bucket, key + other);
    }

    throw new UnsupportedOperationException(
        String.format("Unexpected StandardResolveOptions [%s]", resolveOptions));
  }

  private S3ResourceId resolveFromUri(String uri) {
    S3ResourceId id = fromUri(uri);
    checkArgument(
        id.getScheme().equals(scheme),
        "Cannot resolve a URI as a child resource unless its scheme is [%s]; instead it was [%s]",
        scheme,
        id.getScheme());
    return id;
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    }
    return fromComponents(scheme, getBucket(), key.substring(0, key.lastIndexOf('/') + 1));
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public @Nullable String getFilename() {
    if (!isDirectory()) {
      return key.substring(key.lastIndexOf('/') + 1);
    }
    if ("/".equals(key)) {
      return null;
    }
    String keyWithoutTrailingSlash = key.substring(0, key.length() - 1);
    return keyWithoutTrailingSlash.substring(keyWithoutTrailingSlash.lastIndexOf('/') + 1);
  }

  @Override
  public boolean isDirectory() {
    return key.endsWith("/");
  }

  boolean isWildcard() {
    return GLOB_PREFIX.matcher(getKey()).matches();
  }

  String getKeyNonWildcardPrefix() {
    Matcher m = GLOB_PREFIX.matcher(getKey());
    checkArgument(m.matches(), String.format("Glob expression: [%s] is not expandable.", getKey()));
    return m.group("PREFIX");
  }

  @Override
  public String toString() {
    return String.format("%s://%s%s", scheme, bucket, key);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof S3ResourceId)) {
      return false;
    }
    S3ResourceId o = (S3ResourceId) obj;

    return scheme.equals(o.scheme) && bucket.equals(o.bucket) && key.equals(o.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, bucket, key);
  }
}
