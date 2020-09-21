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

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/**
 * Helper Class based on {@link Row}, it provides Metadata associated with each Record when reading
 * from file(s) using {@link ContextualTextIO}.
 *
 * <h3>Fields:</h3>
 *
 * <ul>
 *   <li>recordOffset: The offset of a record (the byte at which the record begins) in a file. This
 *       information can be useful if you wish to reconstruct the file. {@link
 *       RecordWithMetadata#RANGE_OFFSET}
 *   <li>recordNum: The ordinal number of the record in its file. {@link
 *       RecordWithMetadata#RECORD_NUM}
 *   <li>recordValue: The value / contents of the record {@link RecordWithMetadata#VALUE}
 *   <li>rangeOffset: The starting offset of the range (split), which contained the record, when the
 *       record was read. {@link RecordWithMetadata#RANGE_OFFSET}
 *   <li>recordNumInOffset: The record number relative to the Range. (line number within the range)
 *       {@link RecordWithMetadata#RECORD_NUM_IN_OFFSET}
 *   <li>fileName: Name of the file to which the record belongs (this is the full filename,
 *       eg:path/to/file.txt) {@link RecordWithMetadata#RESOURCE_ID}
 * </ul>
 */
public class RecordWithMetadata {

  public static final String RECORD_OFFSET = "recordOffset";
  public static final String RECORD_NUM = "recordNum";
  public static final String VALUE = "value";
  public static final String RANGE_OFFSET = "rangeOffSet";
  public static final String RECORD_NUM_IN_OFFSET = "recordNumInOffset";
  public static final String RESOURCE_ID = "resourceID";

  public static Schema getSchema() {
    return Schema.builder()
        .addInt64Field(RECORD_OFFSET)
        .addInt64Field(RECORD_NUM)
        .addStringField(VALUE)
        .addInt64Field(RANGE_OFFSET)
        .addInt64Field(RECORD_NUM_IN_OFFSET)
        .addLogicalTypeField(RESOURCE_ID, new ResourceIdRow())
        .build();
  }

  /** A Logical type using Row to represent the ResourceId type. */
  public static class ResourceIdRow implements LogicalType<ResourceId, Row> {

    @Override
    public Schema.FieldType getArgumentType() {
      return Schema.FieldType.STRING;
    }

    @Override
    public String getArgument() {
      return "";
    }

    // The underlying schema used to represent rows.
    private final Schema schema =
        Schema.builder()
            .addStringField("schema")
            .addStringField("fileName")
            .addBooleanField("isDirectory")
            .build();

    @Override
    public String getIdentifier() {
      return "beam:logical_type:resourceIdRow:v1";
    }

    @Override
    public FieldType getBaseType() {
      return FieldType.row(schema);
    }

    // Convert the representation type to the underlying Row type. Called by Beam when necessary.
    @Override
    public Row toBaseType(org.apache.beam.sdk.io.fs.ResourceId resourceId) {
      return Row.withSchema(schema)
          .withFieldValue("schema", resourceId.getScheme())
          .withFieldValue("fileName", resourceId.getFilename())
          .withFieldValue("isDirectory", resourceId.isDirectory())
          .build();
    }

    // Convert the underlying Row type to an Instant. Called by Beam when necessary.
    @Override
    public ResourceId toInputType(Row base) {
      Preconditions.checkNotNull(base.getBoolean("isDirectory"));
      Preconditions.checkNotNull(base.getString("fileName"));
      return FileSystems.matchNewResource(
          base.getString("fileName"), base.getBoolean("isDirectory"));
    }
  }
}
