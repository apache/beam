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
package org.apache.beam.sdk.extensions.python;

import java.util.Arrays;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.util.construction.BaseExternalTest;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConsecutivePythonTransformsTest extends BaseExternalTest {

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testConsecutivePythonMaps() {
    PCollection<?> col =
        (PCollection<?>)
            testPipeline
                .apply(Create.of(-1L, 2L, 3L))
                .apply(
                    "ExternalMap1",
                    PythonExternalTransform.from("apache_beam.Map", expansionAddr)
                        .withArgs(PythonCallableSource.of("lambda x: 'negative' if x < 0 else x")));
    col =
        (PCollection<?>)
            col.apply(
                "ExternalMap2",
                PythonExternalTransform.from("apache_beam.Map", expansionAddr)
                    .withArgs(PythonCallableSource.of("type")));
    col =
        (PCollection<?>)
            col.apply(
                "ExternalMap3",
                PythonExternalTransform.from("apache_beam.Map", expansionAddr)
                    .withArgs(PythonCallableSource.of("str"))
                    .withOutputCoder(StringUtf8Coder.of()));

    PAssert.that((PCollection<String>) col)
        .containsInAnyOrder(Arrays.asList("<class 'str'>", "<class 'int'>", "<class 'int'>"));
  }
}
