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
package org.apache.beam.sdk.testing.junit5;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipelineHandler;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@WithApacheBeam(skipMissingRunnerTests = true)
// just until we can fix the project setup and surefire config
@RunWith(JUnitPlatform.class)
class WithApacheBeamTest {
    @BeamInject
    private TestPipelineHandler<?> pipeline;

    @Test
    @Tag("NeedsRunner")
    @Category(NeedsRunner.class) // for now we go through surefire and therefore junit4 API
    void simplePipeline() { // this test is trivial and just validates the extension setup/filter
        final PCollection<String> apply = pipeline.apply(Create.of("a", "b"));
        PAssert.that(apply).containsInAnyOrder("a", "b");
        pipeline.run();
    }
}
