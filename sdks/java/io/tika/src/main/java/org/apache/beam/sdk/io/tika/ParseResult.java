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
package org.apache.beam.sdk.io.tika;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.util.SerializableThrowable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.tika.metadata.Metadata;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The result of parsing a single file with Tika: contains the file's location, metadata, extracted
 * text, and optionally an error. If there is an error, the metadata and extracted text may be
 * partial (i.e. not represent the entire file).
 */
public class ParseResult implements Serializable {
  private final String fileLocation;
  private final String content;
  private final Metadata metadata;
  private final String[] metadataNames;
  private final @Nullable SerializableThrowable error;

  public static ParseResult success(String fileLocation, String content, Metadata metadata) {
    return new ParseResult(fileLocation, content, metadata, null);
  }

  public static ParseResult success(String fileLocation, String content) {
    return new ParseResult(fileLocation, content, new Metadata(), null);
  }

  public static ParseResult failure(
      String fileLocation, String partialContent, Metadata partialMetadata, Throwable error) {
    return new ParseResult(fileLocation, partialContent, partialMetadata, error);
  }

  private ParseResult(String fileLocation, String content, Metadata metadata, Throwable error) {
    checkArgument(fileLocation != null, "fileLocation can not be null");
    checkArgument(content != null, "content can not be null");
    checkArgument(metadata != null, "metadata can not be null");
    this.fileLocation = fileLocation;
    this.content = content;
    this.metadata = metadata;
    this.metadataNames = metadata.names();
    this.error = (error == null) ? null : new SerializableThrowable(error);
  }

  /** Returns the absolute path to the input file. */
  public String getFileLocation() {
    return fileLocation;
  }

  /** Returns whether this file was parsed successfully. */
  public boolean isSuccess() {
    return error == null;
  }

  /** Returns the parse error, if the file was parsed unsuccessfully. */
  public Throwable getError() {
    checkState(error != null, "This is a successful ParseResult");
    return error.getThrowable();
  }

  /**
   * Same as {@link #getError}, but returns the complete stack trace of the error as a {@link
   * String}.
   */
  public String getErrorAsString() {
    return Throwables.getStackTraceAsString(getError());
  }

  /** Returns the extracted text. May be partial, if this parse result contains a failure. */
  public String getContent() {
    return content;
  }

  /** Returns the extracted metadata. May be partial, if this parse result contains a failure. */
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getFileLocation(),
        getContent(),
        getMetadataHashCode(),
        isSuccess() ? "" : Throwables.getStackTraceAsString(getError()));
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ParseResult)) {
      return false;
    }

    ParseResult other = (ParseResult) obj;
    return Objects.equals(getFileLocation(), other.getFileLocation())
        && Objects.equals(getContent(), other.getContent())
        && Objects.equals(getMetadata(), other.getMetadata())
        && (isSuccess()
            ? other.isSuccess()
            : (!other.isSuccess() && Objects.equals(getErrorAsString(), other.getErrorAsString())));
  }

  // TODO: Remove this function and use metadata.hashCode() once Apache Tika 1.17 gets released.
  private int getMetadataHashCode() {
    int hashCode = 0;
    for (String name : metadataNames) {
      hashCode += name.hashCode() ^ Arrays.hashCode(metadata.getValues(name));
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fileLocation", fileLocation)
        .add("content", "<" + content.length() + " chars>")
        .add("metadata", metadata)
        .add("error", getError() == null ? null : Throwables.getStackTraceAsString(getError()))
        .toString();
  }
}
