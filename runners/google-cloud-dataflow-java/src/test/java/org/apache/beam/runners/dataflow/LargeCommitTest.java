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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LargeCommitTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class})
  public void testLargeCommit() {
    // Prepare 5 values, each ~50MB, to test large commit sizes (~250MB total)
    final int valueSizeMB = 50;
    final int numValues = 5;
    String largeValue = createLargeString('a', valueSizeMB << 20);
    List<KV<String, String>> inputValues = generateKeyValuePairs("key", largeValue, numValues);

    PCollection<KV<String, Iterable<String>>> result =
        p.apply(Create.of(inputValues))
            .apply(GroupByKey.create()); // Group all large values by a single key

    PAssert.that(result)
        .satisfies(
            kvs -> {
              // Validate that only one key exists
              Iterator<KV<String, Iterable<String>>> iterator = kvs.iterator();
              assertTrue(iterator.hasNext());
              KV<String, Iterable<String>> outputKV = iterator.next();
              assertFalse(iterator.hasNext()); // Ensure no additional keys exist

              // Validate key and values
              assertEquals("key", outputKV.getKey());
              assertThat(
                  outputKV.getValue(),
                  Matchers.contains(largeValue, largeValue, largeValue, largeValue, largeValue));
              return null;
            });
    p.run();
  }

  /** Creates a large string of specified {@param size}. */
  private static String createLargeString(char c, int size) {
    char[] buffer = new char[size];
    Arrays.fill(buffer, c);
    return new String(buffer);
  }

  /** Generates {@param count} KV pairs with a single {@param key} and {@param value}. */
  private static List<KV<String, String>> generateKeyValuePairs(
      String key, String value, int count) {
    List<KV<String, String>> keyValuePairs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      keyValuePairs.add(KV.of(key, value));
    }
    return keyValuePairs;
  }
}
