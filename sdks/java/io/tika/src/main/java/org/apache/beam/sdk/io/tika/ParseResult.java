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

import org.apache.tika.metadata.Metadata;

/**
 * Tika parse result containing the file location, metadata
 * and content converted to String.
 */
public class ParseResult implements Serializable {
  private static final long serialVersionUID = 6133510503781405912L;
  private String content;
  private Metadata metadata;
  private String fileLocation;

  public ParseResult() {
  }

  public ParseResult(String fileLocation, String content) {
    this(fileLocation, content, new Metadata());
  }

  public ParseResult(String fileLocation, String content, Metadata metadata) {
    this.fileLocation = fileLocation;
    this.content = content;
    this.metadata = metadata;
  }

  /**
   * Gets a file content.
   */
  public String getContent() {
    return content;
  }

  /**
   * Sets a file content.
   */
  public void setContent(String content) {
    this.content = content;
  }

  /**
   * Gets a file metadata.
   */
  public Metadata getMetadata() {
    return metadata;
  }

  /**
   * Sets a file metadata.
   */
  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  /**
   * Gets a file location.
   */
  public String getFileLocation() {
    return fileLocation;
  }

  /**
   * Sets a file location.
   */
  public void setFileLocation(String fileLocation) {
    this.fileLocation = fileLocation;
  }

  @Override
  public int hashCode() {
    return fileLocation.hashCode() + 37 * content.hashCode() + 37 * metadata.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ParseResult)) {
      return false;
    }

    ParseResult pr = (ParseResult) obj;
    return this.fileLocation.equals(pr.fileLocation)
      && this.content.equals(pr.content)
      && isMetadataEqual(this.metadata, pr.metadata);
  }

  private static boolean isMetadataEqual(Metadata m1, Metadata m2) {
    String[] names = m1.names();
    if (names.length != m2.names().length) {
      return false;
    }
    for (String n : names) {
      String v1 = m1.get(n);
      String v2 = m2.get(n);
      if (!v1.equals(v2)) {
        return false;
      }
    }
    return true;
  }
}
