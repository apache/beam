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

import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Wrapper for invoking external Python RunInference. */
public class RunInference extends PTransform<PCollection<?>, PCollection<Row>> {
  private final String modelLoader;
  private final Schema schema;
  private final Map<String, Object> kwargs;
  private final String expansionService;

  /**
   * Instantiates a multi-language wrapper for a Python RunInference with a given model loader.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param exampleType A schema field type for the example column in output rows.
   * @param inferenceType A schema field type for the inference column in output rows.
   * @return A {@link RunInference} for the given model loader.
   */
  public static RunInference of(
      String modelLoader, Schema.FieldType exampleType, Schema.FieldType inferenceType) {
    Schema schema =
        Schema.of(
            Schema.Field.of("example", exampleType), Schema.Field.of("inference", inferenceType));
    return new RunInference(modelLoader, schema, ImmutableMap.of(), "");
  }

  /**
   * Instantiates a multi-language wrapper for a Python RunInference with a given model loader.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param schema A schema for output rows.
   * @return A {@link RunInference} for the given model loader.
   */
  public static RunInference of(String modelLoader, Schema schema) {
    return new RunInference(modelLoader, schema, ImmutableMap.of(), "");
  }

  /**
   * Sets keyword arguments for the model loader.
   *
   * @return A {@link RunInference} with keyword arguments.
   */
  public RunInference withKwarg(String key, Object arg) {
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder().putAll(kwargs).put(key, arg);
    return new RunInference(modelLoader, schema, builder.build(), expansionService);
  }

  /**
   * Sets an expansion service endpoint for RunInference.
   *
   * @param expansionService A URL for a Python expansion service.
   * @return A {@link RunInference} for the given expansion service endpoint.
   */
  public RunInference withExpansionService(String expansionService) {
    return new RunInference(modelLoader, schema, kwargs, expansionService);
  }

  private RunInference(
      String modelLoader, Schema schema, Map<String, Object> kwargs, String expansionService) {
    this.modelLoader = modelLoader;
    this.schema = schema;
    this.kwargs = kwargs;
    this.expansionService = expansionService;
  }

  @Override
  public PCollection<Row> expand(PCollection<?> input) {
    return input.apply(
        PythonExternalTransform.<PCollection<?>, PCollection<Row>>from(
                "apache_beam.ml.inference.base.RunInference.from_callable", expansionService)
            .withKwarg("model_handler_provider", PythonCallableSource.of(modelLoader))
            .withKwargs(kwargs)
            .withOutputCoder(RowCoder.of(schema)));
  }
}
