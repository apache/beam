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
import java.util.List;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.construction.BaseExternalTest;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the Java RunInference wrapper against the Python test expansion service.
 *
 * <p>The test expansion service attaches a small sklearn model to the Python environment as a
 * staged file named {@code sklearn_model} (see
 * sdks/python/apache_beam/runners/portability/expansion_service_test.py). Where the Python SDK
 * harness puts staged files is only known on the worker, so the tests pass the staged file name and
 * resolve the full path when the model is loaded.
 */
@RunWith(JUnit4.class)
public class RunInferenceTransformTest extends BaseExternalTest {
  private static final String STAGED_MODEL_NAME = "sklearn_model";

  // A SklearnModelHandlerNumpy whose model_uri is the name of a file staged with the Python
  // environment. load_model() runs on the worker, after the SDK harness has set
  // SEMI_PERSISTENT_DIRECTORY.
  private static final String STAGED_MODEL_HANDLER =
      "import os\n"
          + "from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy\n"
          + "class StagedSklearnModelHandlerNumpy(SklearnModelHandlerNumpy):\n"
          + "  def __init__(self, model_uri, *args, **kwargs):\n"
          + "    super().__init__(model_uri, *args, **kwargs)\n"
          + "    self._staged_name = model_uri\n"
          + "  def load_model(self):\n"
          + "    staged_dir = os.environ.get('SEMI_PERSISTENT_DIRECTORY')\n"
          + "    if not staged_dir:\n"
          + "      raise RuntimeError(\n"
          + "          'SEMI_PERSISTENT_DIRECTORY is not set, cannot locate staged file '\n"
          + "          + self._staged_name)\n"
          + "    self.update_model_path(\n"
          + "        os.path.join(staged_dir, 'staged', self._staged_name))\n"
          + "    return super().load_model()\n";

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testRunInference() {
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
                RunInference.of(STAGED_MODEL_HANDLER, schema)
                    .withKwarg("model_uri", STAGED_MODEL_NAME)
                    .withExpansionService(expansionAddr));
    PAssert.that(col).containsInAnyOrder(row0, row1);
  }

  private String getModelLoaderScriptWithKVs() {
    String s = STAGED_MODEL_HANDLER;
    s = s + "from apache_beam.ml.inference.base import KeyedModelHandler\n";
    s = s + "def get_model_handler(model_uri):\n";
    s = s + "  return KeyedModelHandler(StagedSklearnModelHandlerNumpy(model_uri))\n";

    return s;
  }

  static class KVFn extends SimpleFunction<Iterable<Long>, KV<Long, Iterable<Long>>> {
    @Override
    public KV<Long, Iterable<Long>> apply(Iterable<Long> input) {
      Long key = (Long) ((List) input).get(0);
      return KV.of(key, input);
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void testRunInferenceWithKVs() {
    Schema schema =
        Schema.of(
            Schema.Field.of("example", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("inference", Schema.FieldType.INT32));
    Row row0 = Row.withSchema(schema).addArray(0L, 0L).addValue(0).build();
    Row row1 = Row.withSchema(schema).addArray(1L, 1L).addValue(1).build();
    PCollection<Row> col =
        testPipeline
            .apply(Create.<Iterable<Long>>of(Arrays.asList(0L, 0L), Arrays.asList(1L, 1L)))
            .apply(MapElements.via(new KVFn()))
            .setCoder(KvCoder.of(VarLongCoder.of(), IterableCoder.of(VarLongCoder.of())))
            .apply(
                RunInference.ofKVs(getModelLoaderScriptWithKVs(), schema, VarLongCoder.of())
                    .withKwarg("model_uri", STAGED_MODEL_NAME)
                    .withExpansionService(expansionAddr))
            .apply(Values.<Row>create());

    PAssert.that(col).containsInAnyOrder(row0, row1);
  }
}
