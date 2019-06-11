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

import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link AvroBucketMetadata}. */
public class AvroBucketMetadataTest {

  private static final Schema LOCATION_SCHEMA =
      Schema.createRecord(
          "Location",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("currentCountry", Schema.create(Schema.Type.STRING), "", ""),
              new Schema.Field(
                  "prevCountries",
                  Schema.createArray(Schema.create(Schema.Type.STRING)),
                  "",
                  Collections.<String>emptyList())));

  private static final Schema RECORD_SCHEMA =
      Schema.createRecord(
          "Record",
          "",
          "org.apache.beam.sdk.extensions.smb.avro",
          false,
          Lists.newArrayList(
              new Schema.Field("id", Schema.create(Schema.Type.LONG), "", 0L),
              new Schema.Field("location", LOCATION_SCHEMA, "", Collections.emptyList())));

  @Test
  public void testGenericRecord() throws Exception {
    final GenericRecord location =
        new GenericRecordBuilder(LOCATION_SCHEMA)
            .set("currentCountry", "US")
            .set("prevCountries", Arrays.asList("CN", "MX"))
            .build();

    final GenericRecord user =
        new GenericRecordBuilder(RECORD_SCHEMA).set("id", 10L).set("location", location).build();

    Assert.assertEquals(
        (Long) 10L,
        new AvroBucketMetadata<>(1, 1, Long.class, HashType.MURMUR3_32, "id").extractKey(user));

    Assert.assertEquals(
        "US",
        new AvroBucketMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "location.currentCountry")
            .extractKey(user));

    /*
    FIXME: BucketMetadata should allow custom coder?
    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        new AvroBucketMetadata<>(
                1, 1, ArrayList.class, HashType.MURMUR3_32, "location.prevCountries")
            .extractKey(user));
     */
  }

  @Test
  public void testSpecificRecord() throws Exception {
    final AvroGeneratedUser user = new AvroGeneratedUser("foo", 50, "green");

    Assert.assertEquals(
        "green",
        new AvroBucketMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "favorite_color")
            .extractKey(user));

    Assert.assertEquals(
        (Integer) 50,
        new AvroBucketMetadata<>(1, 1, Integer.class, HashType.MURMUR3_32, "favorite_number")
            .extractKey(user));
  }

  @Test
  public void testCoding() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, 1, 1, String.class, HashType.MURMUR3_32, "favorite_color");

    final BucketMetadata<String, GenericRecord> copy = BucketMetadata.from(metadata.toString());
    Assert.assertEquals(metadata.getVersion(), copy.getVersion());
    Assert.assertEquals(metadata.getNumBuckets(), copy.getNumBuckets());
    Assert.assertEquals(metadata.getNumShards(), copy.getNumShards());
    Assert.assertEquals(metadata.getKeyClass(), copy.getKeyClass());
    Assert.assertEquals(metadata.getHashType(), copy.getHashType());
  }

  @Test
  public void testVersionDefault() throws Exception {
    final AvroBucketMetadata<String, GenericRecord> metadata =
        new AvroBucketMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "favorite_color");

    Assert.assertEquals(BucketMetadata.CURRENT_VERSION, metadata.getVersion());
  }
}
