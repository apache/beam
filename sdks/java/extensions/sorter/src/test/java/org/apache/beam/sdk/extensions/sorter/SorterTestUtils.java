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
package org.apache.beam.sdk.extensions.sorter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.junit.rules.ExpectedException;

/** A set of basic tests for {@link Sorter}s. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class SorterTestUtils {

  public static void testEmpty(Sorter sorter) throws Exception {
    assertThat(sorter.sort(), is(emptyIterable()));
  }

  @SuppressWarnings("unchecked")
  public static void testSingleElement(Sorter sorter) throws Exception {
    KV<byte[], byte[]> kv = KV.of(new byte[] {4, 7}, new byte[] {1, 2});
    sorter.add(kv);
    assertThat(sorter.sort(), contains(kv));
  }

  @SuppressWarnings("unchecked")
  public static void testEmptyKeyValueElement(Sorter sorter) throws Exception {
    KV<byte[], byte[]> kv = KV.of(new byte[] {}, new byte[] {});
    sorter.add(kv);
    assertThat(sorter.sort(), contains(kv));
  }

  @SuppressWarnings("unchecked")
  public static void testMultipleIterations(Sorter sorter) throws Exception {
    KV<byte[], byte[]>[] kvs =
        new KV[] {
          KV.of(new byte[] {0}, new byte[] {}),
          KV.of(new byte[] {0, 1}, new byte[] {}),
          KV.of(new byte[] {1}, new byte[] {})
        };
    sorter.add(kvs[1]);
    sorter.add(kvs[2]);
    sorter.add(kvs[0]);
    Iterable<KV<byte[], byte[]>> sorted = sorter.sort();
    assertThat(sorted, contains(kvs[0], kvs[1], kvs[2]));
    // Iterate second time.
    assertThat(sorted, contains(kvs[0], kvs[1], kvs[2]));
  }

  /** Class that generates a new sorter. Used when performance testing multiple sorter creation. */
  interface SorterGenerator {
    Sorter generateSorter();
  }

  /**
   * Generates random records and executes a test with the provided number of sorters and number of
   * records per sorter.
   */
  public static void testRandom(
      SorterGenerator sorterGenerator, int numSorters, int numRecordsPerSorter) throws Exception {
    long start = System.currentTimeMillis();
    for (int i = 0; i < numSorters; ++i) {
      Sorter sorter = sorterGenerator.generateSorter();
      Random rnd = new Random(0L);
      for (int j = 0; j < numRecordsPerSorter; ++j) {
        byte[] key = new byte[8];
        byte[] value = new byte[8];
        rnd.nextBytes(key);
        rnd.nextBytes(value);
        sorter.add(KV.of(key, value));
      }

      byte[] prevKey = null;
      for (KV<byte[], byte[]> record : sorter.sort()) {
        assertTrue(
            prevKey == null
                || UnsignedBytes.lexicographicalComparator().compare(prevKey, record.getKey()) < 0);
        prevKey = record.getKey();
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(
        "Took "
            + (end - start)
            + "ms for "
            + numRecordsPerSorter * numSorters * 1000.0 / (end - start)
            + " records/s");
  }

  /** Tests trying to call add after calling sort. Should throw an exception. */
  public static void testAddAfterSort(Sorter sorter, ExpectedException thrown) throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(is("Records can only be added before sort()"));
    KV<byte[], byte[]> kv = KV.of(new byte[] {4, 7}, new byte[] {1, 2});
    sorter.add(kv);
    sorter.sort();
    sorter.add(kv);
  }

  /** Tests trying to calling sort twice. Should throw an exception. */
  public static void testSortTwice(Sorter sorter, ExpectedException thrown) throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(is("sort() can only be called once."));
    KV<byte[], byte[]> kv = KV.of(new byte[] {4, 7}, new byte[] {1, 2});
    sorter.add(kv);
    sorter.sort();
    sorter.sort();
  }
}
