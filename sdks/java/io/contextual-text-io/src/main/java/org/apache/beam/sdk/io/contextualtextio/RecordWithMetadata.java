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
 *   <li>recordNum: The ordinal number of the record in its file. {@link
 *       RecordWithMetadata#getRecordNum()}
 *   <li>recordValue: The value / contents of the record. {@link RecordWithMetadata#getValue()}
 *   <li>rangeOffset: The starting offset of the range (split), which contained the record, when the
 *       record was read. {@link RecordWithMetadata#getRangeOffset()}
 *   <li>recordNumInOffset: The record number relative to the Range. (line number within the range)
 *       {@link RecordWithMetadata#getRecordNumInOffset()}
 *   <li>fileName: Name of the file to which the record belongs (this is the full filename,
 *       eg:path/to/file.txt). {@link RecordWithMetadata#getFileName()}
 * </ul>
 */
@Experimental(Experimental.Kind.SCHEMAS)
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class RecordWithMetadata {
  /**
   * Returns the offset of the record (the byte at which the record begins) in a file. This
   * information can be useful if you wish to reconstruct the file.
   */
  public abstract long getRecordOffset();

  /** Returns the ordinal number of the record in its file. */
  public abstract long getRecordNum();

  /** Returns the value / content of the Record */
  public abstract String getValue();

  /**
   * Returns the starting offset of the range (split), which contained the record, when the record
   * was read.
   */
  public abstract long getRangeOffset();

  /** Returns the record number relative to the Range. */
  public abstract long getRecordNumInOffset();

  /**
   * Returns the name of the file to which the record belongs (this is the full filename,
   * eg:path/to/file.txt).
   */
  public abstract String getFileName();

  public abstract Builder toBuilder();

  public static Builder newBuilder() {
    return new AutoValue_RecordWithMetadata.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRecordNum(long lineNum);

    public abstract Builder setRecordOffset(long recordOffset);

    public abstract Builder setValue(String value);

    public abstract Builder setFileName(String fileName);

    public abstract Builder setRecordNumInOffset(long recordNumInOffset);

    public abstract Builder setRangeOffset(long startingOffset);

    public abstract RecordWithMetadata build();
  }
}
