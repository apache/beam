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

import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
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
    // 5 50MB values shuffling to a single key
    String value = bigString('a', 50 << 20);
    KV<String, String> kv = KV.of("a", value);
    PCollection<KV<String, Iterable<String>>> result =
        p.apply(Create.of(kv, kv, kv, kv, kv)).apply(GroupByKey.create());

    PAssert.that(result)
        .satisfies(
            kvs -> {
              assertTrue(kvs.iterator().hasNext());
              KV<String, Iterable<String>> outputKV = kvs.iterator().next();
              assertFalse(kvs.iterator().hasNext());
              assertEquals("a", outputKV.getKey());
              assertThat(outputKV.getValue(), Matchers.contains(value, value, value, value, value));
              return null;
            });
    p.run();
  }

  private static String bigString(char c, int size) {
    char[] buf = new char[size];
    for (int i = 0; i < size; i++) {
      buf[i] = c;
    }
    return new String(buf);
  }
}
