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
package org.apache.beam.sdk.extensions.smb.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.SortedBucketSourceJoinBuilder.JoinSource;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;

/** Abstracts SMB sources and sinks for Avro-typed values. */
public class AvroSortedBucketIO {

  public static <KeyT> SortedBucketSink<KeyT, GenericRecord> sink(
      AvroBucketMetadata<KeyT, GenericRecord> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Schema schema) {
    return SortedBucketIO.sink(
        bucketingMetadata,
        outputDirectory,
        ".avro",
        tempDirectory,
        AvroFileOperations.forGenericRecord(schema));
  }

  public static <KeyT, ValueT extends SpecificRecordBase> SortedBucketSink<KeyT, ValueT> sink(
      AvroBucketMetadata<KeyT, ValueT> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Class<ValueT> recordClass) {
    return SortedBucketIO.sink(
        bucketingMetadata,
        outputDirectory,
        ".avro",
        tempDirectory,
        AvroFileOperations.forSpecificRecord(recordClass));
  }

  // Joins
  public static <KeyT> JoinSource<KeyT, GenericRecord> avroSource(
      TupleTag<GenericRecord> tupleTag, Schema schema, ResourceId filenamePrefix) {
    return new JoinSource<>(
        new BucketedInput<>(
            tupleTag,
            filenamePrefix,
            ".avro",
            AvroFileOperations.forGenericRecord(schema).createReader()),
        AvroCoder.of(schema));
  }

  public static <KeyT, ValueT extends SpecificRecordBase> JoinSource<KeyT, ValueT> avroSource(
      TupleTag<ValueT> tupleTag, Class<ValueT> recordClass, ResourceId filenamePrefix) {
    return new JoinSource<>(
        new BucketedInput<>(
            tupleTag,
            filenamePrefix,
            ".avro",
            AvroFileOperations.forSpecificRecord(recordClass).createReader()),
        AvroCoder.of(recordClass));
  }
}
