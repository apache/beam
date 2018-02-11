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
package org.apache.beam.sdk.extensions.scripting;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for the ScriptingParDo transform. */
public class ScriptingParDoTest {
  @Rule public volatile TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testScriptingWithJs() {
    final PCollection<Integer> out =
        pipeline
            .apply(Create.of("v1", "v22"))
            .apply(
                new ScriptingParDo<String, Integer>() {}.withLanguage("js")
                    .withScript("context.output(context.element().length());"));
    PAssert.that(out).containsInAnyOrder(2, 3);
    final PipelineResult result = pipeline.run();
    assertEquals(PipelineResult.State.DONE, result.waitUntilFinish());
  }
}
