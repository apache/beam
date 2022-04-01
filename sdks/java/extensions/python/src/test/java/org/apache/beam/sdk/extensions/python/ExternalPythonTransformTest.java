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

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExternalPythonTransformTest implements Serializable {
  @Ignore("BEAM-14148")
  @Test
  public void trivialPythonTransform() {
    Pipeline p = Pipeline.create();
    PCollection<String> output =
        p.apply(Create.of(KV.of("A", "x"), KV.of("A", "y"), KV.of("B", "z")))
            .apply(
                new ExternalPythonTransform<
                    PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>(
                    "apache_beam.GroupByKey", Row.nullRow(Schema.of()), Row.nullRow(Schema.of())))
            .apply(MapElements.into(TypeDescriptors.strings()).via(kv -> kv.getKey()));
    PAssert.that(output).containsInAnyOrder("A", "B");
    // TODO: Run this on a multi-language supporting runner.
  }
}
