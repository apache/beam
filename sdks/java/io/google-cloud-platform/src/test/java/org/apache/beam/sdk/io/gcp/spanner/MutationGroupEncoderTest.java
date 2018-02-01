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
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link MutationGroupEncoder}.
 */
public class MutationGroupEncoderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private SpannerSchema allTypesSchema;

  @Before
  public void setUp() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "intkey", "INT64");
    builder.addKeyPart("test", "intkey", false);

    builder.addColumn("test", "bool", "BOOL");
    builder.addColumn("test", "int64", "INT64");
    builder.addColumn("test", "float64", "FLOAT64");
    builder.addColumn("test", "string", "STRING");
    builder.addColumn("test", "bytes", "BYTES");
    builder.addColumn("test", "timestamp", "TIMESTAMP");
    builder.addColumn("test", "date", "DATE");

    builder.addColumn("test", "nullbool", "BOOL");
    builder.addColumn("test", "nullint64", "INT64");
    builder.addColumn("test", "nullfloat64", "FLOAT64");
    builder.addColumn("test", "nullstring", "STRING");
    builder.addColumn("test", "nullbytes", "BYTES");
    builder.addColumn("test", "nulltimestamp", "TIMESTAMP");
    builder.addColumn("test", "nulldate", "DATE");

    builder.addColumn("test", "arrbool", "ARRAY<BOOL>");
    builder.addColumn("test", "arrint64", "ARRAY<INT64>");
    builder.addColumn("test", "arrfloat64", "ARRAY<FLOAT64>");
    builder.addColumn("test", "arrstring", "ARRAY<STRING>");
    builder.addColumn("test", "arrbytes", "ARRAY<BYTES>");
    builder.addColumn("test", "arrtimestamp", "ARRAY<TIMESTAMP>");
    builder.addColumn("test", "arrdate", "ARRAY<DATE>");

    builder.addColumn("test", "nullarrbool", "ARRAY<BOOL>");
    builder.addColumn("test", "nullarrint64", "ARRAY<INT64>");
    builder.addColumn("test", "nullarrfloat64", "ARRAY<FLOAT64>");
    builder.addColumn("test", "nullarrstring", "ARRAY<STRING>");
    builder.addColumn("test", "nullarrbytes", "ARRAY<BYTES>");
    builder.addColumn("test", "nullarrtimestamp", "ARRAY<TIMESTAMP>");
    builder.addColumn("test", "nullarrdate", "ARRAY<DATE>");

    allTypesSchema = builder.build();
  }

  @Test
  public void testAllTypesSingleMutation() throws Exception {
    encodeAndVerify(g(appendAllTypes(Mutation.newInsertOrUpdateBuilder("test")).build()));
    encodeAndVerify(g(appendAllTypes(Mutation.newInsertBuilder("test")).build()));
    encodeAndVerify(g(appendAllTypes(Mutation.newUpdateBuilder("test")).build()));
    encodeAndVerify(g(appendAllTypes(Mutation.newReplaceBuilder("test")).build()));
  }

  @Test
  public void testAllTypesMultipleMutations() throws Exception {
    encodeAndVerify(g(
        appendAllTypes(Mutation.newInsertOrUpdateBuilder("test")).build(),
        appendAllTypes(Mutation.newInsertBuilder("test")).build(),
        appendAllTypes(Mutation.newUpdateBuilder("test")).build(),
        appendAllTypes(Mutation.newReplaceBuilder("test")).build(),
        Mutation
            .delete("test", KeySet.range(KeyRange.closedClosed(Key.of(1L), Key.of(2L))))));
  }

  @Test
  public void testUnknownColumn() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "bool_field", "BOOL");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.newInsertBuilder("test").set("unknown")
        .to(true).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Columns [unknown] were not defined in table test");
    encodeAndVerify(g(mutation), schema);
  }

  @Test
  public void testUnknownTable() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "bool_field", "BOOL");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.newInsertBuilder("unknown").set("bool_field")
        .to(true).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unknown table 'unknown'");
    encodeAndVerify(g(mutation), schema);
  }

  @Test
  public void testMutationCaseInsensitive() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "bool_field", "BOOL");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.newInsertBuilder("TEsT").set("BoOL_FiELd").to(true).build();
    encodeAndVerify(g(mutation), schema);
  }

  @Test
  public void testDeleteCaseInsensitive() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "int_field", "INT64");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.delete("TeSt", Key.of(1L));
    encodeAndVerify(g(mutation), schema);
  }

  @Test
  public void testDeletes() throws Exception {
    encodeAndVerify(g(Mutation.delete("test", Key.of(1L))));
    encodeAndVerify(g(Mutation.delete("test", Key.of((Long) null))));

    KeySet allTypes = KeySet.newBuilder()
        .addKey(Key.of(1L))
        .addKey(Key.of((Long) null))
        .addKey(Key.of(1.2))
        .addKey(Key.of((Double) null))
        .addKey(Key.of("one"))
        .addKey(Key.of((String) null))
        .addKey(Key.of(ByteArray.fromBase64("abcd")))
        .addKey(Key.of((ByteArray) null))
        .addKey(Key.of(Timestamp.now()))
        .addKey(Key.of((Timestamp) null))
        .addKey(Key.of(Date.fromYearMonthDay(2012, 1, 1)))
        .addKey(Key.of((Date) null))
        .build();

    encodeAndVerify(g(Mutation.delete("test", allTypes)));

    encodeAndVerify(
        g(Mutation
            .delete("test", KeySet.range(KeyRange.closedClosed(Key.of(1L), Key.of(2L))))));
  }

  private Mutation.WriteBuilder appendAllTypes(Mutation.WriteBuilder builder) {
    Timestamp ts = Timestamp.now();
    Date date = Date.fromYearMonthDay(2017, 1, 1);
    return builder
        .set("bool").to(true)
        .set("int64").to(1L)
        .set("float64").to(1.0)
        .set("string").to("my string")
        .set("bytes").to(ByteArray.fromBase64("abcdedf"))
        .set("timestamp").to(ts)
        .set("date").to(date)

        .set("arrbool").toBoolArray(Arrays.asList(true, false, null, true, null, false))
        .set("arrint64").toInt64Array(Arrays.asList(10L, -12L, null, null, 100000L))
        .set("arrfloat64").toFloat64Array(Arrays.asList(10., -12.23, null, null, 100000.33231))
        .set("arrstring").toStringArray(Arrays.asList("one", "two", null, null, "three"))
        .set("arrbytes").toBytesArray(Arrays.asList(ByteArray.fromBase64("abcs"), null))
        .set("arrtimestamp").toTimestampArray(Arrays.asList(Timestamp.MIN_VALUE, null, ts))
        .set("arrdate").toDateArray(Arrays.asList(null, date))

        .set("nullbool").to((Boolean) null)
        .set("nullint64").to((Long) null)
        .set("nullfloat64").to((Double) null)
        .set("nullstring").to((String) null)
        .set("nullbytes").to((ByteArray) null)
        .set("nulltimestamp").to((Timestamp) null)
        .set("nulldate").to((Date) null)

        .set("nullarrbool").toBoolArray((Iterable<Boolean>) null)
        .set("nullarrint64").toInt64Array((Iterable<Long>) null)
        .set("nullarrfloat64").toFloat64Array((Iterable<Double>) null)
        .set("nullarrstring").toStringArray(null)
        .set("nullarrbytes").toBytesArray(null)
        .set("nullarrtimestamp").toTimestampArray(null)
        .set("nullarrdate").toDateArray(null);
  }

  @Test
  public void int64Keys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "INT64");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "INT64");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(1L)
            .set("keydesc").to(0L)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2L)
            .set("keydesc").to((Long) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2L)
            .set("keydesc").to(10L)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2L)
            .set("keydesc").to(9L)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to((Long) null)
            .set("keydesc").to(0L)
            .build());

    List<Key> keys = Arrays.asList(
        Key.of(1L, 0L),
        Key.of(2L, null),
        Key.of(2L, 10L),
        Key.of(2L, 9L),
        Key.of(2L, 0L)
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void float64Keys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "FLOAT64");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "FLOAT64");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(1.0)
            .set("keydesc").to(0.)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2.)
            .set("keydesc").to((Long) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2.)
            .set("keydesc").to(10.)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2.)
            .set("keydesc").to(9.)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(2.)
            .set("keydesc").to(0.)
            .build());
    List<Key> keys = Arrays.asList(
        Key.of(1., 0.),
        Key.of(2., null),
        Key.of(2., 10.),
        Key.of(2., 9.),
        Key.of(2., 0.)
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void stringKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "STRING");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "STRING");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to("a")
            .set("keydesc").to("bc")
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to("b")
            .set("keydesc").to((String) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to("b")
            .set("keydesc").to("z")
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to("b")
            .set("keydesc").to("y")
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to("b")
            .set("keydesc").to("a")
            .build());

    List<Key> keys = Arrays.asList(
        Key.of("a", "bc"),
        Key.of("b", null),
        Key.of("b", "z"),
        Key.of("b", "y"),
        Key.of("b", "a")
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void bytesKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "BYTES");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "BYTES");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(ByteArray.fromBase64("abc"))
            .set("keydesc").to(ByteArray.fromBase64("zzz"))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(ByteArray.fromBase64("xxx"))
            .set("keydesc").to((ByteArray) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(ByteArray.fromBase64("xxx"))
            .set("keydesc").to(ByteArray.fromBase64("zzzz"))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(ByteArray.fromBase64("xxx"))
            .set("keydesc").to(ByteArray.fromBase64("ssss"))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(ByteArray.fromBase64("xxx"))
            .set("keydesc").to(ByteArray.fromBase64("aaa"))
            .build());

    List<Key> keys = Arrays.asList(
        Key.of(ByteArray.fromBase64("abc"), ByteArray.fromBase64("zzz")),
        Key.of(ByteArray.fromBase64("xxx"), null),
        Key.of(ByteArray.fromBase64("xxx"), ByteArray.fromBase64("zzz")),
        Key.of(ByteArray.fromBase64("xxx"), ByteArray.fromBase64("sss")),
        Key.of(ByteArray.fromBase64("xxx"), ByteArray.fromBase64("aaa"))
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void dateKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "DATE");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "DATE");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Date.fromYearMonthDay(2012, 10, 10))
            .set("keydesc").to(Date.fromYearMonthDay(2000, 10, 10))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Date.fromYearMonthDay(2020, 10, 10))
            .set("keydesc").to((Date) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Date.fromYearMonthDay(2020, 10, 10))
            .set("keydesc").to(Date.fromYearMonthDay(2050, 10, 10))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Date.fromYearMonthDay(2020, 10, 10))
            .set("keydesc").to(Date.fromYearMonthDay(2000, 10, 10))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Date.fromYearMonthDay(2020, 10, 10))
            .set("keydesc").to(Date.fromYearMonthDay(1900, 10, 10))
            .build());

    List<Key> keys = Arrays.asList(
        Key.of(Date.fromYearMonthDay(2012, 10, 10), ByteArray.fromBase64("zzz")),
        Key.of(Date.fromYearMonthDay(2015, 10, 10), null),
        Key.of(Date.fromYearMonthDay(2015, 10, 10), Date.fromYearMonthDay(2050, 10, 10)),
        Key.of(Date.fromYearMonthDay(2015, 10, 10), Date.fromYearMonthDay(2000, 10, 10)),
        Key.of(Date.fromYearMonthDay(2015, 10, 10), Date.fromYearMonthDay(1900, 10, 10))
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void timestampKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "key", "TIMESTAMP");
    builder.addKeyPart("test", "key", false);

    builder.addColumn("test", "keydesc", "TIMESTAMP");
    builder.addKeyPart("test", "keydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Timestamp.ofTimeMicroseconds(10000))
            .set("keydesc").to(Timestamp.ofTimeMicroseconds(50000))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Timestamp.ofTimeMicroseconds(20000))
            .set("keydesc").to((Timestamp) null)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Timestamp.ofTimeMicroseconds(20000))
            .set("keydesc").to(Timestamp.ofTimeMicroseconds(90000))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Timestamp.ofTimeMicroseconds(20000))
            .set("keydesc").to(Timestamp.ofTimeMicroseconds(50000))
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("key").to(Timestamp.ofTimeMicroseconds(20000))
            .set("keydesc").to(Timestamp.ofTimeMicroseconds(10000))
            .build());


    List<Key> keys = Arrays.asList(
        Key.of(Timestamp.ofTimeMicroseconds(10000), ByteArray.fromBase64("zzz")),
        Key.of(Timestamp.ofTimeMicroseconds(20000), null),
        Key.of(Timestamp.ofTimeMicroseconds(20000), Timestamp.ofTimeMicroseconds(90000)),
        Key.of(Timestamp.ofTimeMicroseconds(20000), Timestamp.ofTimeMicroseconds(50000)),
        Key.of(Timestamp.ofTimeMicroseconds(20000), Timestamp.ofTimeMicroseconds(10000))
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  @Test
  public void boolKeys() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "boolkey", "BOOL");
    builder.addKeyPart("test", "boolkey", false);

    builder.addColumn("test", "boolkeydesc", "BOOL");
    builder.addKeyPart("test", "boolkeydesc", true);

    SpannerSchema schema = builder.build();

    List<Mutation> mutations = Arrays.asList(
        Mutation.newInsertOrUpdateBuilder("test")
            .set("boolkey").to(true)
            .set("boolkeydesc").to(false)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("boolkey").to(false)
            .set("boolkeydesc").to(false)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("boolkey").to(false)
            .set("boolkeydesc").to(true)
            .build(),
        Mutation.newInsertOrUpdateBuilder("test")
            .set("boolkey").to((Boolean) null)
            .set("boolkeydesc").to(false)
            .build()
    );

    List<Key> keys = Arrays.asList(
        Key.of(true, ByteArray.fromBase64("zzz")),
        Key.of(false, null),
        Key.of(false, false),
        Key.of(false, true),
        Key.of(null, false)
    );

    verifyEncodedOrdering(schema, mutations);
    verifyEncodedOrdering(schema, "test", keys);
  }

  private void verifyEncodedOrdering(SpannerSchema schema, List<Mutation> mutations) {
    MutationGroupEncoder encoder = new MutationGroupEncoder(schema);
    List<byte[]> mutationEncodings = new ArrayList<>(mutations.size());
    for (Mutation m : mutations) {
      mutationEncodings.add(encoder.encodeKey(m));
    }
    List<byte[]> copy = new ArrayList<>(mutationEncodings);
    copy.sort(UnsignedBytes.lexicographicalComparator());

    Assert.assertEquals(mutationEncodings, copy);
  }

  private void verifyEncodedOrdering(SpannerSchema schema, String table, List<Key> keys) {
    MutationGroupEncoder encoder = new MutationGroupEncoder(schema);
    List<byte[]> keyEncodings = new ArrayList<>(keys.size());
    for (Key k : keys) {
      keyEncodings.add(encoder.encodeKey(table, k));
    }
    List<byte[]> copy = new ArrayList<>(keyEncodings);
    copy.sort(UnsignedBytes.lexicographicalComparator());

    Assert.assertEquals(keyEncodings, copy);
  }

  private MutationGroup g(Mutation mutation, Mutation... other) {
    return MutationGroup.create(mutation, other);
  }

  private void encodeAndVerify(MutationGroup expected) {
    SpannerSchema schema = this.allTypesSchema;
    encodeAndVerify(expected, schema);
  }

  private static void encodeAndVerify(MutationGroup expected, SpannerSchema schema) {
    MutationGroupEncoder coder = new MutationGroupEncoder(schema);
    byte[] encode = coder.encode(expected);
    MutationGroup actual = coder.decode(encode);

    Assert.assertTrue(mutationGroupsEqual(expected, actual));
  }

  private static boolean mutationGroupsEqual(MutationGroup a, MutationGroup b) {
    ImmutableList<Mutation> alist = ImmutableList.copyOf(a);
    ImmutableList<Mutation> blist = ImmutableList.copyOf(b);

    if (alist.size() != blist.size()) {
      return false;
    }

    for (int i = 0; i < alist.size(); i++) {
      if (!mutationsEqual(alist.get(i), blist.get(i))) {
        return false;
      }
    }
    return true;
  }

  // Is different from Mutation#equals. Case insensitive for table/column names, the order of
  // the columns doesn't matter.
  private static boolean mutationsEqual(Mutation a, Mutation b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (a.getOperation() != b.getOperation()) {
      return false;
    }
    if (!a.getTable().equalsIgnoreCase(b.getTable())) {
      return false;
    }
    if (a.getOperation() == Mutation.Op.DELETE) {
      return a.getKeySet().equals(b.getKeySet());
    }

    // Compare pairs instead? This seems to be good enough...
    return ImmutableSet.copyOf(getNormalizedColumns(a))
        .equals(ImmutableSet.copyOf(getNormalizedColumns(b))) && ImmutableSet.copyOf(a.getValues())
        .equals(ImmutableSet.copyOf(b.getValues()));
  }

  // Pray for Java 8 support.
  private static Iterable<String> getNormalizedColumns(Mutation a) {
    return Iterables.transform(a.getColumns(), String::toLowerCase);
  }
}
