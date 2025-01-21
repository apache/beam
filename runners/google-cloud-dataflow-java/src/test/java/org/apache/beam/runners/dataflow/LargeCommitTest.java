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

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LargeCommitTest implements Serializable {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class})
  public void testLargeCommit() {
    PCollection<KV<String, Iterable<String>>> result =
        p.apply(Create.of("starterElement"))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String value = bigString('a', 50 << 20);
                        KV<String, String> kv = KV.of("a", value);
                        // 5 50MB values shuffling to a single key
                        for (int i = 0; i < 5; i++) {
                          c.output(kv);
                        }
                      }
                    }))
            .apply(GroupByKey.create());

    PAssert.that(result)
        .satisfies(
            kvs -> {
              String value = bigString('a', 50 << 20);
              List<KV<String, Iterable<String>>> outputKVs = new ArrayList<>();
              Iterables.addAll(outputKVs, kvs);
              assertEquals(1, outputKVs.size());
              KV<String, Iterable<String>> outputKV = outputKVs.get(0);
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
