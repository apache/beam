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
package org.apache.beam.sdk.extensions.smb;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroSortedBucketFile;
import org.junit.Test;

/** Tests bucket metadata coding. */
public class BucketMetadataTest {

  // @Todo
  @Test
  public void testMetadataCoding() throws Exception {
    BucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(10, String.class, BucketMetadata.HashType.MURMUR3_32, "a.b.c");
    System.out.println("==========");
    System.out.println(metadata);

    metadata = BucketMetadata.from(metadata.toString());
    System.out.println("==========");
    System.out.println(metadata.getNumBuckets());
    System.out.println(metadata.getSortingKeyClass());
    System.out.println(metadata.getHashType());
    System.out.println(metadata.getSortingKeyCoder());

    SerializableCoder<AvroSortedBucketFile> coder =
        SerializableCoder.of(AvroSortedBucketFile.class);

    Schema schema = Schema.createRecord("Record", null, null, false);

    AvroSortedBucketFile<GenericRecord> file = new AvroSortedBucketFile<>(null, schema);

    System.out.println(coder);
    System.out.println(file);
    // CoderUtils.decodeFromBase64(coder, CoderUtils.encodeToBase64(coder, file));
  }
}
