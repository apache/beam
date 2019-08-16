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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import com.alibaba.fastjson.JSONObject;
import com.google.cloud.datacatalog.Entry;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * Utils to handle GCS entries from Cloud Data Catalog.
 *
 * <p>At the moment everything here is a brittle hack because there doesn't seem to be an actual
 * proto available for this yet in the client library, so we have to do something like parsing this
 * manually, or generating our own protos/clients.
 */
class GcsUtils {

  /**
   * 'gcs_fileset_spec' field id, as defined in .proto.
   *
   * <p>Until we get the updated proto or a generated client this is the most straightforward way of
   * extracting data from it.
   */
  private static final int GCS_FILESET_SPEC = 6;

  /** Check if the entry represents a GCS fileset in Data Catalog. */
  static boolean isGcs(Entry entry) {
    // 'gcs_fileset_spec' is not in the generated client yet
    // so we have to manually parse it from unknown fields
    return entry.getUnknownFields().hasField(GCS_FILESET_SPEC);
  }

  /** Creates a Beam SQL table description from a GCS fileset entry. */
  static Table.Builder tableBuilder(Entry entry) {
    UnknownFieldSet.Field gcsFilesetSpec = entry.getUnknownFields().getField(GCS_FILESET_SPEC);
    List<ByteString> filesetFields = gcsFilesetSpec.getLengthDelimitedList();

    // We support exactly one 'file_patterns' field and nothing else at the moment
    if (filesetFields.size() != 1) {
      throw new UnsupportedOperationException(
          "Unable to parse GCS entry '" + entry.getName() + "'");
    }
    return readStringField(filesetFields.get(0));
  }

  private static Table.Builder readStringField(ByteString filesetField) {
    try {

      // TODO: Fix as soon as updated Data Catalog proto becomes available,
      //  replace with actual field access on a generated class.

      CodedInputStream codedInputStream = filesetField.newCodedInput();
      codedInputStream.readRawVarint64(); // consume string length prefix

      String filePattern = codedInputStream.readStringRequireUtf8();

      if (!filePattern.startsWith("gs://")) {
        throw new UnsupportedOperationException(
            "Unsupported file pattern. "
                + "Only file patterns with 'gs://' schema are supported at the moment.");
      }

      return Table.builder()
          .type("text")
          .location(filePattern)
          .properties(new JSONObject())
          .comment("");

    } catch (IOException e) {
      throw new UnsupportedOperationException(
          "Unable to parse a GCS fileset Entry from Data Catalog");
    }
  }
}
