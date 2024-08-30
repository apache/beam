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
package org.apache.beam.sdk.providers;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.providers.GenerateSequenceSchemaTransformProvider.GenerateSequenceConfiguration;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GenerateSequenceSchemaTransformProviderTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testGenerateSequence() {
    GenerateSequenceConfiguration config =
        GenerateSequenceConfiguration.builder().setStart(0L).setEnd(10L).build();
    SchemaTransform sequence = new GenerateSequenceSchemaTransformProvider().from(config);

    List<Row> expected = new ArrayList<>(10);
    for (long i = 0L; i < 10L; i++) {
      expected.add(
          Row.withSchema(GenerateSequenceSchemaTransformProvider.OUTPUT_SCHEMA)
              .withFieldValue("value", i)
              .build());
    }

    PAssert.that(
            PCollectionRowTuple.empty(p)
                .apply(sequence)
                .get(GenerateSequenceSchemaTransformProvider.OUTPUT_ROWS_TAG))
        .containsInAnyOrder(expected);
    p.run();
  }
}
