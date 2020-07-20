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
package org.apache.beam.sdk.io.aws.s3;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

class S3ResourceId implements ResourceId {

  static final String SCHEME = "s3";

  private static final Pattern S3_URI =
      Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<KEY>.*))?");

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  private final String bucket;
  private final String key;
  private final Long size;
  private final Date lastModified;

  private S3ResourceId(
      String bucket, String key, @Nullable Long size, @Nullable Date lastModified) {
    checkArgument(!Strings.isNullOrEmpty(bucket), "bucket");
    checkArgument(!bucket.contains("/"), "bucket must not contain '/': [%s]", bucket);
    this.bucket = bucket;
    this.key = checkNotNull(key, "key");
    this.size = size;
    this.lastModified = lastModified;
  }

  static S3ResourceId fromComponents(String bucket, String key) {
    if (!key.startsWith("/")) {
      key = "/" + key;
    }
    return new S3ResourceId(bucket, key, null, null);
  }

  static S3ResourceId fromUri(String uri) {
    Matcher m = S3_URI.matcher(uri);
    checkArgument(m.matches(), "Invalid S3 URI: [%s]", uri);
    checkArgument(m.group("SCHEME").equalsIgnoreCase(SCHEME), "Invalid S3 URI scheme: [%s]", uri);
    String bucket = m.group("BUCKET");
    String key = Strings.nullToEmpty(m.group("KEY"));
    if (!key.startsWith("/")) {
      key = "/" + key;
    }
    return fromComponents(bucket, key);
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
    return new S3ResourceId(bucket, key, size, lastModified);
  }

  Optional<Date> getLastModified() {
    return Optional.fromNullable(lastModified);
  }

  S3ResourceId withLastModified(Date lastModified) {
    return new S3ResourceId(bucket, key, size, lastModified);
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(isDirectory(), "Expected this resource to be a directory, but was [%s]", toString());

    if (resolveOptions == StandardResolveOptions.RESOLVE_DIRECTORY) {
      if ("..".equals(other)) {
        if ("/".equals(key)) {
          return this;
        }
        int parentStopsAt = key.substring(0, key.length() - 1).lastIndexOf('/');
        return fromComponents(bucket, key.substring(0, parentStopsAt + 1));
      }

      if ("".equals(other)) {
        return this;
      }

      if (!other.endsWith("/")) {
        other += "/";
      }
      if (S3_URI.matcher(other).matches()) {
        return fromUri(other);
      }
      return fromComponents(bucket, key + other);
    }

    if (resolveOptions == StandardResolveOptions.RESOLVE_FILE) {
      checkArgument(
          !other.endsWith("/"), "Cannot resolve a file with a directory path: [%s]", other);
      checkArgument(!"..".equals(other), "Cannot resolve parent as file: [%s]", other);
      if (S3_URI.matcher(other).matches()) {
        return fromUri(other);
      }
      return fromComponents(bucket, key + other);
    }

    throw new UnsupportedOperationException(
        String.format("Unexpected StandardResolveOptions [%s]", resolveOptions));
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    }
    return fromComponents(getBucket(), key.substring(0, key.lastIndexOf('/') + 1));
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Nullable
  @Override
  public String getFilename() {
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
    return String.format("%s://%s%s", SCHEME, bucket, key);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof S3ResourceId)) {
      return false;
    }

    return bucket.equals(((S3ResourceId) obj).bucket) && key.equals(((S3ResourceId) obj).key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, key);
  }
}
