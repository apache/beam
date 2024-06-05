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
package org.apache.beam.sdk.extensions.python.transforms;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.BaseExternalTest;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PythonMapTest extends BaseExternalTest {
  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testPythonMap() {
    PCollection<String> output =
        testPipeline
            .apply("CreateData", Create.of(ImmutableList.of("a", "b", "c", "d")))
            .apply(
                "ApplyPythonMap",
                PythonMap.<String, String>viaMapFn("lambda x:3*x", StringUtf8Coder.of())
                    .withExpansionService(expansionAddr));
    PAssert.that(output).containsInAnyOrder("aaa", "bbb", "ccc", "ddd");
  }

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testPythonFlatMap() {
    PCollection<String> output =
        testPipeline
            .apply("CreateData", Create.of(ImmutableList.of("a", "b", "c")))
            .apply(
                "ApplyPythonFlatMap",
                PythonMap.<String, String>viaFlatMapFn(
                        "lambda x:[2*x, 3*x, 4*x]", StringUtf8Coder.of())
                    .withExpansionService(expansionAddr));
    PAssert.that(output)
        .containsInAnyOrder("aa", "aaa", "aaaa", "bb", "bbb", "bbbb", "cc", "ccc", "cccc");
  }
}
