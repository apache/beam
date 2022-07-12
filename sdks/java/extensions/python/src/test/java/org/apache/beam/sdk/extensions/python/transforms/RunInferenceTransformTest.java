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

import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.runners.core.construction.BaseExternalTest;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RunInferenceTransformTest extends BaseExternalTest {
  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testRunInference() {
    String stagingLocation =
        Optional.ofNullable(System.getProperty("semiPersistDir")).orElse("/tmp");
    Schema schema =
        Schema.of(
            Schema.Field.of("example", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("inference", Schema.FieldType.INT32));
    Row row0 = Row.withSchema(schema).addArray(0L, 0L).addValue(0).build();
    Row row1 = Row.withSchema(schema).addArray(1L, 1L).addValue(1).build();
    PCollection<Row> col =
        testPipeline
            .apply(Create.<Iterable<Long>>of(Arrays.asList(0L, 0L), Arrays.asList(1L, 1L)))
            .setCoder(IterableCoder.of(VarLongCoder.of()))
            .apply(
                RunInference.of(
                        "apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy",
                        schema)
                    .withKwarg(
                        // The test expansion service creates the test model and saves it to the
                        // returning external environment as a dependency.
                        // (sdks/python/apache_beam/runners/portability/expansion_service_test.py)
                        // The dependencies for Python SDK harness are supposed to be staged to
                        // $SEMI_PERSIST_DIR/staged directory.
                        "model_uri", String.format("%s/staged/sklearn_model", stagingLocation))
                    .withExpansionService(expansionAddr));
    PAssert.that(col).containsInAnyOrder(row0, row1);
  }
}
