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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.TreeMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MutationKeyEncoder}. */
@RunWith(JUnit4.class)
public class MutationKeyEncoderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void tableNameOrdering() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test1", "key", "INT64");
    builder.addKeyPart("test1", "key", false);

    builder.addColumn("test2", "key", "INT64");
    builder.addKeyPart("test2", "key", false);

    SpannerSchema schema = builder.build();

    // Verify that the encoded keys are ordered by table name then key
    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test1").set("key").to(1L).build(),
            Mutation.newInsertOrUpdateBuilder("test1").set("key").to(2L).build(),
            Mutation.newInsertOrUpdateBuilder("test1").set("key").to((Long) null).build(),
            Mutation.newInsertOrUpdateBuilder("test2").set("key").to(1L).build(),
            Mutation.newInsertOrUpdateBuilder("test2").set("key").to(2L).build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void int64Keys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "INT64");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "INT64");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(1L)
                .set("keydesc")
                .to(0L)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2L)
                .set("keydesc")
                .to((Long) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2L)
                .set("keydesc")
                .to(10L)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2L)
                .set("keydesc")
                .to(9L)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to((Long) null)
                .set("keydesc")
                .to(0L)
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void float64Keys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "FLOAT64");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "FLOAT64");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(1.0)
                .set("keydesc")
                .to(0.)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2.)
                .set("keydesc")
                .to((Long) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2.)
                .set("keydesc")
                .to(10.)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2.)
                .set("keydesc")
                .to(9.)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(2.)
                .set("keydesc")
                .to(0.)
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void stringKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "STRING");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "STRING");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("a")
                .set("keydesc")
                .to("bc")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                .set("keydesc")
                .to((String) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                .set("keydesc")
                .to("z")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                .set("keydesc")
                .to("y")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                .set("keydesc")
                .to("a")
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void bytesKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "BYTES");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "BYTES");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(ByteArray.fromBase64("abc"))
                .set("keydesc")
                .to(ByteArray.fromBase64("zzz"))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(ByteArray.fromBase64("xxx"))
                .set("keydesc")
                .to((ByteArray) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(ByteArray.fromBase64("xxx"))
                .set("keydesc")
                .to(ByteArray.fromBase64("zzzz"))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(ByteArray.fromBase64("xxx"))
                .set("keydesc")
                .to(ByteArray.fromBase64("ssss"))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(ByteArray.fromBase64("xxx"))
                .set("keydesc")
                .to(ByteArray.fromBase64("aaa"))
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void dateKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "DATE");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "DATE");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Date.fromYearMonthDay(2012, 10, 10))
                .set("keydesc")
                .to(Date.fromYearMonthDay(2000, 10, 10))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Date.fromYearMonthDay(2020, 10, 10))
                .set("keydesc")
                .to((Date) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Date.fromYearMonthDay(2020, 10, 10))
                .set("keydesc")
                .to(Date.fromYearMonthDay(2050, 10, 10))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Date.fromYearMonthDay(2020, 10, 10))
                .set("keydesc")
                .to(Date.fromYearMonthDay(2000, 10, 10))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Date.fromYearMonthDay(2020, 10, 10))
                .set("keydesc")
                .to(Date.fromYearMonthDay(1900, 10, 10))
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void timestampKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "TIMESTAMP");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "TIMESTAMP");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Timestamp.ofTimeMicroseconds(10000))
                .set("keydesc")
                .to(Timestamp.ofTimeMicroseconds(50000))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Timestamp.ofTimeMicroseconds(20000))
                .set("keydesc")
                .to((Timestamp) null)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Timestamp.ofTimeMicroseconds(20000))
                .set("keydesc")
                .to(Timestamp.ofTimeMicroseconds(90000))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Timestamp.ofTimeMicroseconds(20000))
                .set("keydesc")
                .to(Timestamp.ofTimeMicroseconds(50000))
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to(Timestamp.ofTimeMicroseconds(20000))
                .set("keydesc")
                .to(Timestamp.ofTimeMicroseconds(10000))
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void boolKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "boolkey", "BOOL");
    builder.addKeyPart("test", "boolkey", false);

    builder.addColumn("test", "boolkeydesc", "BOOL");
    builder.addKeyPart("test", "boolkeydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("boolkey")
                .to(true)
                .set("boolkeydesc")
                .to(false)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("boolkey")
                .to(true)
                .set("boolkeydesc")
                .to(true)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("boolkey")
                .to(false)
                .set("boolkeydesc")
                .to(false)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("boolkey")
                .to(false)
                .set("boolkeydesc")
                .to(true)
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("boolkey")
                .to((Boolean) null)
                .set("boolkeydesc")
                .to(false)
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void unspecifiedStringKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "STRING");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "STRING");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("a")
                .set("keydesc")
                .to("b")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("a")
                .set("keydesc")
                .to("a")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                // leave keydesc value unspecified --> maxvalue descending.
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                .set("key")
                .to("b")
                .set("keydesc")
                .to("a")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test")
                // leave 'key' value unspecified -> maxvalue
                .set("keydesc")
                .to("a")
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void deleteOrdering() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test1", "key", "INT64");
    builder.addKeyPart("test1", "key", false);

    builder.addColumn("test2", "key", "INT64");
    builder.addKeyPart("test2", "key", false);

    SpannerSchema schema = builder.build();

    // Verify that the encoded keys are ordered by table name then key
    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.delete("test1", KeySet.all()), // non-point deletes come first
            Mutation.delete("test1", Key.of(1L)),
            Mutation.delete("test1", Key.of(2L)),
            Mutation.delete("test2", KeySet.prefixRange(Key.of(1L))),
            Mutation.delete("test2", Key.of(2L)));

    verifyEncodedOrdering(schema, sortedMutations);
  }

  @Test
  public void unknownTableOrdering() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test1", "key", "INT64");
    builder.addKeyPart("test1", "key", false);

    SpannerSchema schema = builder.build();

    // Verify that the encoded keys are ordered by table name and column values (as text).
    List<Mutation> sortedMutations =
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("test2")
                .set("key")
                .to("a")
                .set("keydesc")
                .to("a")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test2")
                .set("key")
                .to("a")
                .set("keydesc")
                .to("b")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test3")
                .set("key")
                .to("b")
                // leave keydesc value unspecified --> maxvalue descending.
                .build(),
            Mutation.newInsertOrUpdateBuilder("test4")
                .set("key")
                .to("b")
                .set("keydesc")
                .to("a")
                .build(),
            Mutation.newInsertOrUpdateBuilder("test4")
                // leave 'key' value unspecified -> maxvalue
                .set("keydesc")
                .to("a")
                .build());

    verifyEncodedOrdering(schema, sortedMutations);
    Assert.assertEquals(3, MutationKeyEncoder.getUnknownTablesWarningsMap().size());
    Assert.assertEquals(2, MutationKeyEncoder.getUnknownTablesWarningsMap().get("test2").get());
    Assert.assertEquals(1, MutationKeyEncoder.getUnknownTablesWarningsMap().get("test3").get());
    Assert.assertEquals(2, MutationKeyEncoder.getUnknownTablesWarningsMap().get("test4").get());
  }

  private void verifyEncodedOrdering(SpannerSchema schema, List<Mutation> expectedMutations) {
    MutationKeyEncoder encoder = new MutationKeyEncoder(schema);

    Assert.assertEquals(5, expectedMutations.size());

    // mix them up.
    List<Mutation> unsortedMutations =
        Arrays.asList(
            expectedMutations.get(3),
            expectedMutations.get(4),
            expectedMutations.get(1),
            expectedMutations.get(2),
            expectedMutations.get(0));

    // Use a map to sort the list by encoded table/key, then by Mutation contents to give a defined
    // order when the same key is given, or if it is an unknown table.
    TreeMultimap<byte[], Mutation> mutationsByEncoding =
        TreeMultimap.create(
            UnsignedBytes.lexicographicalComparator(), Comparator.comparing(Mutation::toString));
    for (Mutation m : unsortedMutations) {
      mutationsByEncoding.put(encoder.encodeTableNameAndKey(m), m);
    }

    Assert.assertEquals(expectedMutations, new ArrayList<>(mutationsByEncoding.values()));
  }
}
