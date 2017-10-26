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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.tika.metadata.Metadata;

/**
 * Tika parse result containing the file location, metadata
 * and content converted to String.
 */
@SuppressWarnings("serial")
public class ParseResult implements Serializable {

  private static final class SerializableThrowable implements Serializable {
    private final Throwable throwable;
    private final StackTraceElement[] stackTrace;

    private SerializableThrowable(Throwable t) {
      this.throwable = t;
      this.stackTrace = t.getStackTrace();
    }

    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
      is.defaultReadObject();
      throwable.setStackTrace(stackTrace);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof SerializableThrowable)) {
        return false;
      }
      SerializableThrowable sr = (SerializableThrowable) obj;
      return this.throwable.getClass().equals(sr.throwable.getClass())
          && (this.throwable.getCause() == null && sr.throwable.getCause() == null
            || this.throwable.getCause().getClass().equals(sr.throwable.getCause().getClass()));
    }

    @Override
    public int hashCode() {
      int hashCode = 1;
      hashCode = 31 * hashCode + throwable.getClass().hashCode();
      return hashCode;
    }
  }

  private final String fileLocation;
  @Nullable
  private String content;
  private final Metadata metadata;
  private final String[] metadataNames;
  @Nullable
  private SerializableThrowable throwable;

  public ParseResult(String fileLocation, String content) {
    this(fileLocation, content, new Metadata());
  }

  public ParseResult(String fileLocation, String content, Metadata metadata) {
    this.fileLocation = fileLocation;
    this.content = content;
    this.metadata = metadata;
    this.metadataNames = metadata.names();
  }

  public ParseResult(String fileLocation, Metadata metadata, Throwable t) {
    this.fileLocation = fileLocation;
    this.metadata = metadata;
    this.metadataNames = metadata.names();
    this.throwable = new SerializableThrowable(t);
  }

  /**
   * Gets a file content which can be set to null
   * if a parsing exception has occurred.
   */
  public String getContent() {
    return content;
  }

  /**
   * Gets a file metadata which can be only be partially populated
   * if a parsing exception has occurred.
   */
  public Metadata getMetadata() {
    return metadata;
  }

  /**
   * Gets a file location.
   */
  public String getFileLocation() {
    return fileLocation;
  }

  /**
   * Gets a parse exception.
   */
  public Throwable getThrowable() {
    return throwable == null ? null : throwable.throwable;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;
    hashCode = 31 * hashCode + fileLocation.hashCode();
    if (content != null) {
      hashCode = 31 * hashCode + content.hashCode();
    }
    hashCode = 31 * hashCode + getMetadataHashCode();
    if (throwable != null) {
      hashCode = 31 * hashCode + throwable.hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ParseResult)) {
      return false;
    }

    ParseResult pr = (ParseResult) obj;
    if (this.content == null && pr.content != null
        || this.content != null && pr.content == null
        || this.throwable == null && pr.throwable != null
        || this.throwable != null && pr.throwable == null) {
      return false;
    }

    return this.fileLocation.equals(pr.fileLocation)
      && (this.content == null && pr.content == null || this.content.equals(pr.content))
      && this.metadata.equals(pr.metadata)
      && (this.throwable == null && pr.throwable == null || this.throwable.equals(pr.throwable));
  }

  //TODO:
  // Remove this function and use metadata.hashCode() once Apache Tika 1.17 gets released.
  private int getMetadataHashCode() {
    int hashCode = 0;
    for (String name : metadataNames) {
      hashCode += name.hashCode() ^ Arrays.hashCode(metadata.getValues(name));
    }
    return hashCode;
  }
}
