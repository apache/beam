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
package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Used to pass values around within test pipelines. */
@AutoValue
public abstract class TestRow implements Serializable, Comparable<TestRow> {
  /** Manually create a test row. */
  public static TestRow create(Integer id, String name) {
    return new AutoValue_TestRow(id, name);
  }

  public abstract Integer id();

  public abstract String name();

  @Override
  public int compareTo(TestRow other) {
    return id().compareTo(other.id());
  }

  /** Creates a {@link TestRow} from the seed value. */
  public static TestRow fromSeed(Integer seed) {
    return create(seed, getNameForSeed(seed));
  }

  /** Returns the name field value produced from the given seed. */
  public static String getNameForSeed(Integer seed) {
    return "Testval" + seed;
  }

  /**
   * Returns a range of {@link TestRow}s for seed values between rangeStart (inclusive) and rangeEnd
   * (exclusive).
   */
  public static Iterable<TestRow> getExpectedValues(int rangeStart, int rangeEnd) {
    List<TestRow> ret = new ArrayList<>(rangeEnd - rangeStart + 1);
    for (int i = rangeStart; i < rangeEnd; i++) {
      ret.add(fromSeed(i));
    }
    return ret;
  }

  /** Uses the input Long values as seeds to produce {@link TestRow}s. */
  public static class DeterministicallyConstructTestRowFn extends DoFn<Long, TestRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(fromSeed(c.element().intValue()));
    }
  }

  /** Outputs just the name stored in the {@link TestRow}. */
  public static class SelectNameFn extends DoFn<TestRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().name());
    }
  }

  /**
   * Precalculated hashes - you can calculate an entry by running HashingFn on the name() for the
   * rows generated from seeds in [0, n).
   */
  private static final ImmutableMap<Integer, String> EXPECTED_HASHES =
      ImmutableMap.of(
          1000, "7d94d63a41164be058a9680002914358",
          100_000, "c7cbddb319209e200f1c5eebef8fe960",
          600_000, "e2add2f680de9024e9bc46cd3912545e",
          5_000_000, "c44f8a5648cd9207c9c6f77395a998dc");

  /**
   * Returns the hash value that {@link HashingFn} will return when it is run on {@link TestRow}s
   * produced by getExpectedValues(0, rowCount).
   */
  public static String getExpectedHashForRowCount(int rowCount)
      throws UnsupportedOperationException {
    return getHashForRecordCount(rowCount, EXPECTED_HASHES);
  }
}
