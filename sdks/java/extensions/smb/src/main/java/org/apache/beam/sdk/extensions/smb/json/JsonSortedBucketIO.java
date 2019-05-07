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
package org.apache.beam.sdk.extensions.smb.json;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.ReadBuilder.JoinSource;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.TupleTag;

/** Abstracts SMB sources and sinks for JSON records. */
public class JsonSortedBucketIO {

  public static <KeyT> SortedBucketSink<KeyT, TableRow> sink(
      JsonBucketMetadata<KeyT> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory) {
    return SortedBucketIO.write(
        bucketingMetadata, outputDirectory, ".json", tempDirectory, new JsonFileOperations());
  }

  public static <KeyT> JoinSource<KeyT, TableRow> jsonSource(
      TupleTag<TableRow> tupleTag, ResourceId filenamePrefix) {
    return new JoinSource<>(
        new BucketedInput<>(tupleTag, filenamePrefix, ".json", new JsonFileOperations()),
        TableRowJsonCoder.of());
  }
}
