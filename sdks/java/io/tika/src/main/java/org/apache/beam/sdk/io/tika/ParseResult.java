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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.tika.metadata.Metadata;

/**
 * Tika parse result containing the file location, metadata
 * and content converted to String.
 */
public class ParseResult implements Serializable {
  private static final long serialVersionUID = 6133510503781405912L;
  private final String fileLocation;
  private final String content;
  private final Metadata metadata;
  private final String[] metadataNames;

  public ParseResult(String fileLocation, String content) {
    this(fileLocation, content, new Metadata());
  }

  public ParseResult(String fileLocation, String content, Metadata metadata) {
    this.fileLocation = fileLocation;
    this.content = content;
    this.metadata = metadata;
    this.metadataNames = metadata.names();
  }

  /**
   * Gets a file content.
   */
  public String getContent() {
    return content;
  }

  /**
   * Gets a file metadata.
   */
  public Metadata getMetadata() {
    return getMetadataCopy();
  }

  /**
   * Gets a file location.
   */
  public String getFileLocation() {
    return fileLocation;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;
    hashCode = 31 * hashCode + fileLocation.hashCode();
    hashCode = 31 * hashCode + content.hashCode();
    hashCode = 31 * hashCode + getMetadataHashCode();
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ParseResult)) {
      return false;
    }

    ParseResult pr = (ParseResult) obj;
    return this.fileLocation.equals(pr.fileLocation)
      && this.content.equals(pr.content)
      && this.metadata.equals(pr.metadata);
  }

  private int getMetadataHashCode() {
    int hashCode = 0;
    for (String name : metadataNames) {
      hashCode += name.hashCode() ^ Arrays.hashCode(metadata.getValues(name));
    }
    return hashCode;
  }
  private Metadata getMetadataCopy() {
    Metadata metadataCopy = new Metadata();
    for (String name : metadataNames) {
      for (String value : metadata.getValues(name)) {
        metadataCopy.add(name, value);
      }
    }
    return metadataCopy;
  }
}
