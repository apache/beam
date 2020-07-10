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
package org.apache.beam.sdk.io.azure.blobstore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

class AzfsResourceId implements ResourceId {

  static final String SCHEME = "azfs";
  private static final Pattern AZFS_URI =
      Pattern.compile("(?<SCHEME>[^:]+)://(?<ACCOUNT>[^/]+)/(?<CONTAINER>[^/]+)(?:/(?<BLOB>.*))?");
  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  private final String account;
  private final String container;
  private final String blob;
  private final Long size;
  private final Date lastModified;

  private AzfsResourceId(
      String account,
      String container,
      @Nullable String blob,
      @Nullable Long size,
      @Nullable Date lastModified) {
    // We are assuming that every resource id is either a container or a blob in a container, not
    // just an account. This is because we will not enable users to create Azure containers through
    // beam at this time.
    checkArgument(!Strings.isNullOrEmpty(container), "container");
    checkArgument(!container.contains("/"), "container must not contain '/': [%s]", container);
    this.account = account;
    this.container = container;
    if (blob == null || blob.isEmpty()) {
      this.blob = null;
    } else {
      this.blob = blob;
    }
    this.size = size;
    this.lastModified = lastModified;
  }

  static AzfsResourceId fromComponents(String account, String container, String blob) {
    return new AzfsResourceId(account, container, blob, null, null);
  }

  static AzfsResourceId fromComponents(String account, String container) {
    return new AzfsResourceId(account, container, null, null, null);
  }

  static AzfsResourceId fromUri(String uri) {
    Matcher m = AZFS_URI.matcher(uri);
    checkArgument(m.matches(), "Invalid AZFS URI: [%s]", uri);
    checkArgument(m.group("SCHEME").equalsIgnoreCase(SCHEME), "Invalid AZFS URI scheme: [%s]", uri);
    String account = m.group("ACCOUNT");
    String container = m.group("CONTAINER");
    String blob = m.group("BLOB");
    return fromComponents(account, container, blob);
  }

  public String getAccount() {
    return account;
  }

  public String getContainer() {
    return container;
  }

  public String getBlob() {
    return blob;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  Optional<Long> getSize() {
    return Optional.fromNullable(size);
  }

  AzfsResourceId withSize(long size) {
    return new AzfsResourceId(account, container, blob, size, lastModified);
  }

  Optional<Date> getLastModified() {
    return Optional.fromNullable(lastModified);
  }

  AzfsResourceId withLastModified(Date lastModified) {
    return new AzfsResourceId(account, container, blob, size, lastModified);
  }

  @Override
  public boolean isDirectory() {
    return (blob == null) || (blob.endsWith("/"));
  }

  boolean isWildcard() {
    if (blob == null) {
      return false;
    }
    return GLOB_PREFIX.matcher(blob).matches();
  }

  String getBlobNonWildcardPrefix() {
    Matcher m = GLOB_PREFIX.matcher(getBlob());
    checkArgument(
        m.matches(), String.format("Glob expression: [%s] is not expandable.", getBlob()));
    return m.group("PREFIX");
  }

  public boolean isContainer() {
    return blob == null;
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    }
    if (blob.lastIndexOf('/') == -1) {
      return fromComponents(account, container);
    }
    return fromComponents(account, container, blob.substring(0, blob.lastIndexOf('/') + 1));
  }

  @Nullable
  @Override
  public String getFilename() {
    if (blob == null) {
      return null;
    }
    if (!isDirectory()) {
      return blob.substring(blob.lastIndexOf('/') + 1);
    }
    String blobWithoutTrailingSlash = blob.substring(0, blob.length() - 1);
    return blobWithoutTrailingSlash.substring(blobWithoutTrailingSlash.lastIndexOf('/') + 1);
  }

  @Override
  public String toString() {
    if (blob != null) {
      return String.format("%s://%s/%s/%s", SCHEME, account, container, blob);
    }
    return String.format("%s://%s/%s/", SCHEME, account, container);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AzfsResourceId)) {
      return false;
    }
    return account.equals(((AzfsResourceId) obj).account)
        && container.equals(((AzfsResourceId) obj).container)
        && Objects.equals(blob, ((AzfsResourceId) obj).blob);
  }

  @Override
  public int hashCode() {
    return Objects.hash(account, container, blob);
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(isDirectory(), "Expected this resource to be a directory, but was [%s]", toString());

    if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY) {
      if ("..".equals(other)) {
        if ("/".equals(blob)) {
          return this;
        }
        int parentStopsAt = blob.substring(0, blob.length() - 1).lastIndexOf('/');
        return fromComponents(account, container, blob.substring(0, parentStopsAt + 1));
      }
      if ("".equals(other)) {
        return this;
      }
      if (!other.endsWith("/")) {
        other += "/";
      }
      if (AZFS_URI.matcher(other).matches()) {
        return fromUri(other);
      }
      if (blob == null) {
        return fromComponents(account, container, other);
      }
      return fromComponents(account, container, blob + other);
    }
    if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_FILE) {
      checkArgument(
          !other.endsWith("/"), "Cannot resolve a file with a directory path: [%s]", other);
      checkArgument(!"..".equals(other), "Cannot resolve parent as file: [%s]", other);
      if (AZFS_URI.matcher(other).matches()) {
        return fromUri(other);
      }
      if (blob == null) {
        return fromComponents(account, container, other);
      }
      return fromComponents(account, container, blob + other);
    }
    throw new UnsupportedOperationException(
        String.format("Unexpected StandardResolveOptions [%s]", resolveOptions));
  }
}
