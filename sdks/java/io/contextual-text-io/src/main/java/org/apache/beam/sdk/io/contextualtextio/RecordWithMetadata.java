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
package org.apache.beam.sdk.io.contextualtextio;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Helper Class based on {@link AutoValueSchema}, it provides Metadata associated with each Record
 * when reading from file(s) using {@link ContextualTextIO}.
 *
 * <h3>Fields:</h3>
 *
 * <ul>
 *   <li>recordOffset: The offset of a record (the byte at which the record begins) in a file. This
 *       information can be useful if you wish to reconstruct the file. {@link
 *       RecordWithMetadata#getRecordOffset()}
 *   <li>recordNum: The record number of the record from its ordinal file. {@link
 *       RecordWithMetadata#getRecordNum()}
 *   <li>recordValue: The value / contents of the records {@link
 *       RecordWithMetadata#getRecordValue()}
 *   <li>startingOffset: The starting offset of the range to which the record belongs. {@link
 *       RecordWithMetadata#getStartingOffset()}
 *   <li>recordNumInOffset: The record number relative to the Range. (line number within the range)
 *       {@link RecordWithMetadata#getRecordNumInOffset()}
 *   <li>fileName: Name of the file to which the record belongs (this is the full filename,
 *       eg:path/to/file.txt) {@link RecordWithMetadata#getFileName()}
 * </ul>
 */
@Experimental(Experimental.Kind.SCHEMAS)
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class RecordWithMetadata {
  public abstract Long getRecordOffset();

  public abstract Long getRecordNum();

  public abstract String getRecordValue();

  public abstract Long getStartingOffset();

  public abstract Long getRecordNumInOffset();

  public abstract Builder toBuilder();

  public abstract String getFileName();

  public static Builder newBuilder() {
    return new AutoValue_RecordWithMetadata.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRecordNum(Long lineNum);

    public abstract Builder setRecordOffset(Long recordOffset);

    public abstract Builder setRecordValue(String line);

    public abstract Builder setFileName(String file);

    public abstract Builder setRecordNumInOffset(Long recordNumInOffset);

    public abstract Builder setStartingOffset(Long startingOffset);

    public abstract RecordWithMetadata build();
  }
}
