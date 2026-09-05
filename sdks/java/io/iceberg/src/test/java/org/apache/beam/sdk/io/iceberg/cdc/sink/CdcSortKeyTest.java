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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CdcSortKey}. */
@RunWith(JUnit4.class)
public class CdcSortKeyTest {

  // Ascending sequence numbers, and ValueKinds in ascending kindRank order.
  private static final long[] SEQS_ASCENDING = {-5L, -1L, 0L, 1L, 5L};
  private static final ValueKind[] KINDS_BY_RANK_ASCENDING = {
    ValueKind.UPDATE_BEFORE, ValueKind.DELETE, ValueKind.UPDATE_AFTER, ValueKind.INSERT
  };

  // Primary keys of assorted lengths and leading bytes.
  private static final byte[][] PKS = {{0x02}, {0x01, 0x7F}, {(byte) 0xF0, 0x00, 0x10}};

  @Test
  public void kindRankOrdersBeforeImagesFirst() {
    assertThat(CdcSortKey.kindRank(ValueKind.UPDATE_BEFORE), Matchers.equalTo((byte) 0));
    assertThat(CdcSortKey.kindRank(ValueKind.DELETE), Matchers.equalTo((byte) 1));
    assertThat(CdcSortKey.kindRank(ValueKind.UPDATE_AFTER), Matchers.equalTo((byte) 2));
    assertThat(CdcSortKey.kindRank(ValueKind.INSERT), Matchers.equalTo((byte) 3));
  }

  /**
   * The sorted keys must group each primary key's entries into one unbroken run, ordered by (seq,
   * kind) within the run. Which primary key's run comes first is NOT part of the contract and is
   * deliberately unasserted.
   */
  @Test
  public void sortGroupsEachPkContiguouslyOrderedBySeqThenKindWithin() {
    // Per pk, its keys in expected within-key order: seq ascending, kindRank breaking ties.
    List<List<byte[]>> expectedByPk = new ArrayList<>();
    for (byte[] pk : PKS) {
      List<byte[]> expected = new ArrayList<>();
      for (long seq : SEQS_ASCENDING) {
        for (ValueKind kind : KINDS_BY_RANK_ASCENDING) {
          expected.add(CdcSortKey.encode(pk, seq, kind));
        }
      }
      expectedByPk.add(expected);
    }

    // Feed the sorter the same keys interleaved across pks (the shape a shard group arrives in);
    // the sort must both regroup and reorder.
    List<byte[]> sorted = new ArrayList<>();
    for (int i = 0; i < expectedByPk.get(0).size(); i++) {
      for (List<byte[]> keys : expectedByPk) {
        sorted.add(keys.get(i));
      }
    }
    sorted.sort(UnsignedBytes.lexicographicalComparator());

    // Contiguity: each pk appears in exactly one run.
    Set<Integer> seenRuns = new HashSet<>();
    int previous = -1;
    for (byte[] key : sorted) {
      int pkIndex = pkIndexOf(key);
      if (pkIndex != previous) {
        assertTrue("entries of pk " + pkIndex + " are split across runs", seenRuns.add(pkIndex));
        previous = pkIndex;
      }
    }

    // Within each run, the (seq, kind) order pinned above.
    for (int pkIndex = 0; pkIndex < PKS.length; pkIndex++) {
      List<byte[]> run = new ArrayList<>();
      for (byte[] key : sorted) {
        if (pkIndexOf(key) == pkIndex) {
          run.add(key);
        }
      }
      List<byte[]> expected = expectedByPk.get(pkIndex);
      assertThat(run, Matchers.hasSize(expected.size()));
      for (int i = 0; i < expected.size(); i++) {
        assertArrayEquals(expected.get(i), run.get(i));
      }
    }
  }

  /**
   * Two primary keys where one's bytes are a strict prefix of the other's, with sequence numbers
   * chosen so the unprefixed layout {@code [pkBytes][seq ^ Long.MIN_VALUE:8][kindRank:1]} WOULD
   * interleave them; the length prefix must keep the short key's entries adjacent.
   */
  @Test
  public void lengthPrefixKeepsAPrefixPkContiguous() {
    byte[] shortPk = {0x0A};
    byte[] longPk = {0x0A, (byte) 0x80};
    byte[] shortLow = CdcSortKey.encode(shortPk, 0L, ValueKind.INSERT);
    byte[] shortHigh = CdcSortKey.encode(shortPk, Long.MAX_VALUE, ValueKind.INSERT);
    byte[] longMid = CdcSortKey.encode(longPk, 0L, ValueKind.INSERT);

    // Fixture self-check: with the length prefix stripped, the long pk's entry lands BETWEEN the
    // short pk's two entries (its second byte 0x80 ties the flipped seq 0 and loses to the
    // flipped Long.MAX_VALUE).
    List<byte[]> naive =
        new ArrayList<>(Arrays.asList(stripLengthPrefix(shortHigh), stripLengthPrefix(longMid)));
    naive.add(stripLengthPrefix(shortLow));
    naive.sort(UnsignedBytes.lexicographicalComparator());
    assertArrayEquals(stripLengthPrefix(shortLow), naive.get(0));
    assertArrayEquals(stripLengthPrefix(longMid), naive.get(1));
    assertArrayEquals(stripLengthPrefix(shortHigh), naive.get(2));

    // The real keys: the short pk's entries stay adjacent, low seq first.
    List<byte[]> sorted = new ArrayList<>(Arrays.asList(shortHigh, longMid, shortLow));
    sorted.sort(UnsignedBytes.lexicographicalComparator());
    int low = indexOfKey(sorted, shortLow);
    int high = indexOfKey(sorted, shortHigh);
    assertThat(high, Matchers.equalTo(low + 1));
  }

  @Test
  public void encodeOrdersAcrossLongExtremes() {
    byte[] pk = {0x01};
    byte[] min = CdcSortKey.encode(pk, Long.MIN_VALUE, ValueKind.INSERT);
    byte[] negOne = CdcSortKey.encode(pk, -1L, ValueKind.INSERT);
    byte[] zero = CdcSortKey.encode(pk, 0L, ValueKind.INSERT);
    byte[] one = CdcSortKey.encode(pk, 1L, ValueKind.INSERT);
    byte[] max = CdcSortKey.encode(pk, Long.MAX_VALUE, ValueKind.INSERT);

    List<byte[]> sorted = new ArrayList<>(Arrays.asList(max, zero, min, one, negOne));
    sorted.sort(UnsignedBytes.lexicographicalComparator());

    assertArrayEquals(min, sorted.get(0));
    assertArrayEquals(negOne, sorted.get(1));
    assertArrayEquals(zero, sorted.get(2));
    assertArrayEquals(one, sorted.get(3));
    assertArrayEquals(max, sorted.get(4));
  }

  /**
   * The encoding is a frozen wire format: an in-place pipeline update replays in-flight groups
   * through it, so changing any of these bytes silently re-orders live data. Update only with a
   * migration story.
   */
  @Test
  public void encodePinnedByteLayout() {
    // pk {0x01, 0x02} -> pkLen 2; seq 5 -> flippedSeq 0x8000000000000005; INSERT -> kindRank 3.
    assertArrayEquals(
        new byte[] {0, 0, 0, 2, 1, 2, (byte) 0x80, 0, 0, 0, 0, 0, 0, 5, 3},
        CdcSortKey.encode(new byte[] {1, 2}, 5L, ValueKind.INSERT));
    // pk {0xAB} -> pkLen 1; seq 0 -> flippedSeq 0x8000000000000000; UPDATE_BEFORE -> kindRank 0.
    assertArrayEquals(
        new byte[] {0, 0, 0, 1, (byte) 0xAB, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0},
        CdcSortKey.encode(new byte[] {(byte) 0xAB}, 0L, ValueKind.UPDATE_BEFORE));
  }

  /**
   * {@code samePk} is the writer's block-boundary test: equal for any two keys of one pk whatever
   * their seq/kind bytes, unequal across pks, including the prefix-pk pair whose seq bytes tie.
   */
  @Test
  public void samePkComparesOnlyThePkPrefix() {
    byte[] pk = {0x0A};
    assertTrue(
        CdcSortKey.samePk(
            CdcSortKey.encode(pk, Long.MIN_VALUE, ValueKind.UPDATE_BEFORE),
            CdcSortKey.encode(pk, Long.MAX_VALUE, ValueKind.INSERT)));
    assertFalse(
        CdcSortKey.samePk(
            CdcSortKey.encode(new byte[] {0x01}, 5L, ValueKind.INSERT),
            CdcSortKey.encode(new byte[] {0x02}, 5L, ValueKind.INSERT)));
    // A strict-prefix pk with seq bytes continuing the longer pk's bytes must still differ.
    assertFalse(
        CdcSortKey.samePk(
            CdcSortKey.encode(pk, 0L, ValueKind.INSERT),
            CdcSortKey.encode(new byte[] {0x0A, (byte) 0x80}, 0L, ValueKind.INSERT)));
  }

  /** The index in {@link #PKS} of the pk carried in {@code key}'s length-prefixed prefix. */
  private static int pkIndexOf(byte[] key) {
    int pkLen = ByteBuffer.wrap(key).getInt();
    byte[] pk = Arrays.copyOfRange(key, 4, 4 + pkLen);
    for (int i = 0; i < PKS.length; i++) {
      if (Arrays.equals(PKS[i], pk)) {
        return i;
      }
    }
    throw new AssertionError("unknown pk " + Arrays.toString(pk));
  }

  /** Drops the 4-byte length prefix, leaving the naive {@code [pkBytes][seq][kind]} layout. */
  private static byte[] stripLengthPrefix(byte[] key) {
    return Arrays.copyOfRange(key, 4, key.length);
  }

  private static int indexOfKey(List<byte[]> keys, byte[] key) {
    for (int i = 0; i < keys.size(); i++) {
      if (Arrays.equals(keys.get(i), key)) {
        return i;
      }
    }
    throw new AssertionError("key not found: " + Arrays.toString(key));
  }
}
