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

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/** Tests bucket metadata coding. */
public class AvroBucketMetadataTest {

  // GenericRecord tests

  private static final Schema SCHEMA_NESTED_FIELD =
      Schema.createRecord(
          "location",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Lists.newArrayList(
              new Field("currentCountry", Schema.create(Type.STRING), "", null),
              new Field(
                  "prevCountries", Schema.createArray(Schema.create(Type.STRING)), "", null)));

  private static final Schema SCHEMA =
      Schema.createRecord(
          "user",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Lists.newArrayList(
              new Field("id", Schema.create(Type.LONG), "", null),
              new Field("location", SCHEMA_NESTED_FIELD, "", null)));

  @Test
  public void testGRMetadataCoding() throws Exception {
    final BucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "location.currentCountry");

    final BucketMetadata<String, GenericRecord> roundtripMetadata =
        BucketMetadata.from(metadata.toString());

    Assert.assertEquals(roundtripMetadata.getNumBuckets(), metadata.getNumBuckets());
    Assert.assertEquals(roundtripMetadata.getSortingKeyClass(), metadata.getSortingKeyClass());
    Assert.assertEquals(roundtripMetadata.getHashType(), metadata.getHashType());

    final SerializableCoder<AvroFileOperations> coder =
        SerializableCoder.of(AvroFileOperations.class);
    final AvroFileOperations<GenericRecord> file = new AvroFileOperations<>(null, SCHEMA);
    final AvroFileOperations<GenericRecord> roundtripFile =
        CoderUtils.decodeFromBase64(coder, CoderUtils.encodeToBase64(coder, file));

    Assert.assertEquals(roundtripFile.getRecordClass(), file.getRecordClass());
    Assert.assertEquals(roundtripFile.getSchema(), file.getSchema());
  }

  @Test
  public void testGRExtractSortingKey() throws Exception {
    final GenericRecord location = new Record(SCHEMA_NESTED_FIELD);
    location.put("currentCountry", "US");
    location.put("prevCountries", Arrays.asList("CN", "MX"));

    final GenericRecord user = new Record(SCHEMA);
    user.put("id", 10L);
    user.put("location", location);

    Assert.assertEquals(
        "US",
        new AvroBucketMetadata<>(
                10, String.class, BucketMetadata.HashType.MURMUR3_32, "location.currentCountry")
            .extractSortingKey(user));

    Assert.assertEquals(
        (Long) 10L,
        new AvroBucketMetadata<>(10, Long.class, BucketMetadata.HashType.MURMUR3_32, "id")
            .extractSortingKey(user));

    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        new AvroBucketMetadata<>(
                10, ArrayList.class, BucketMetadata.HashType.MURMUR3_32, "location.prevCountries")
            .extractSortingKey(user));
  }

  @Test
  public void testGRBucketCompatibility() throws Exception {
    final BucketMetadata<String, GenericRecord> m1 =
        new AvroBucketMetadata<>(2, String.class, BucketMetadata.HashType.MURMUR3_32, "name");

    final BucketMetadata<String, GenericRecord> m2 =
        new AvroBucketMetadata<>(2, String.class, BucketMetadata.HashType.MURMUR3_32, "name");

    final BucketMetadata<String, GenericRecord> m3 =
        new AvroBucketMetadata<>(3, String.class, BucketMetadata.HashType.MURMUR3_32, "name");

    Assert.assertTrue(m1.compatibleWith(m2));
    Assert.assertFalse(m1.compatibleWith(m3));
    Assert.assertFalse(m2.compatibleWith(m3));
  }

  // SpecificRecord tests

  @Test
  public void testSRMetadataCoding() throws Exception {
    final BucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "favorite_color");

    final BucketMetadata<String, AvroGeneratedUser> roundtripMetadata =
        BucketMetadata.from(metadata.toString());

    Assert.assertEquals(roundtripMetadata.getNumBuckets(), metadata.getNumBuckets());
    Assert.assertEquals(roundtripMetadata.getSortingKeyClass(), metadata.getSortingKeyClass());
    Assert.assertEquals(roundtripMetadata.getHashType(), metadata.getHashType());

    final SerializableCoder<AvroFileOperations> coder =
        SerializableCoder.of(AvroFileOperations.class);
    final AvroFileOperations<AvroGeneratedUser> file =
        new AvroFileOperations<>(AvroGeneratedUser.class, AvroGeneratedUser.SCHEMA$);
    final AvroFileOperations<AvroGeneratedUser> roundtripFile =
        CoderUtils.decodeFromBase64(coder, CoderUtils.encodeToBase64(coder, file));

    Assert.assertEquals(roundtripFile.getRecordClass(), file.getRecordClass());
    Assert.assertEquals(roundtripFile.getSchema(), file.getSchema());
  }

  @Test
  public void testSRExtractSortingKey() throws Exception {
    final AvroGeneratedUser user = new AvroGeneratedUser("foo", 50, "green");

    Assert.assertEquals(
        "green",
        new AvroBucketMetadata<>(
                10, String.class, BucketMetadata.HashType.MURMUR3_32, "favorite_color")
            .extractSortingKey(user));

    Assert.assertEquals(
        (Integer) 50,
        new AvroBucketMetadata<>(
                10, Integer.class, BucketMetadata.HashType.MURMUR3_32, "favorite_number")
            .extractSortingKey(user));
  }

  @Test
  public void testSRBucketCompatibility() throws Exception {
    final AvroBucketMetadata<String, AvroGeneratedUser> m1 =
        new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "favorite_color");

    final AvroBucketMetadata<String, AvroGeneratedUser> m2 =
        new AvroBucketMetadata<>(
            10, String.class, BucketMetadata.HashType.MURMUR3_32, "favorite_color");

    final AvroBucketMetadata<String, AvroGeneratedUser> m3 =
        new AvroBucketMetadata<>(
            3, String.class, BucketMetadata.HashType.MURMUR3_32, "favorite_color");

    Assert.assertTrue(m1.compatibleWith(m2));
    Assert.assertFalse(m1.compatibleWith(m3));
    Assert.assertFalse(m2.compatibleWith(m3));
  }
}
