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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper for invoking external Python {@code RunInference}. */
public class RunInference<OutputT> extends PTransform<PCollection<?>, PCollection<OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(RunInference.class);

  private final String modelLoader;
  private final Schema schema;
  private final Map<String, Object> kwargs;
  private final String expansionService;
  private final @Nullable Coder<?> keyCoder;
  private final List<String> extraPackages;

  /**
   * Instantiates a multi-language wrapper for a Python RunInference with a given model loader.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param exampleType A schema field type for the example column in output rows.
   * @param inferenceType A schema field type for the inference column in output rows.
   * @return A {@link RunInference} for the given model loader.
   */
  public static RunInference<Row> of(
      String modelLoader, Schema.FieldType exampleType, Schema.FieldType inferenceType) {
    Schema schema =
        Schema.of(
            Schema.Field.of("example", exampleType), Schema.Field.of("inference", inferenceType));
    return new RunInference<>(modelLoader, schema, ImmutableMap.of(), null, "");
  }

  /**
   * Similar to {@link RunInference#of(String, FieldType, FieldType)} but the input is a {@link
   * PCollection} of {@link KV}s.
   *
   * <p>Also outputs a {@link PCollection} of {@link KV}s of the same key type.
   *
   * <p>For example, use this if you are using Python {@code KeyedModelHandler} as the model
   * handler.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param exampleType A schema field type for the example column in output rows.
   * @param inferenceType A schema field type for the inference column in output rows.
   * @param keyCoder a {@link Coder} for the input and output Key type.
   * @param <KeyT> input and output Key type. Inferred by the provided coder.
   * @return A {@link RunInference} for the given model loader.
   */
  public static <KeyT> RunInference<KV<KeyT, Row>> ofKVs(
      String modelLoader,
      Schema.FieldType exampleType,
      Schema.FieldType inferenceType,
      Coder<KeyT> keyCoder) {
    Schema schema =
        Schema.of(
            Schema.Field.of("example", exampleType), Schema.Field.of("inference", inferenceType));
    return new RunInference<>(modelLoader, schema, ImmutableMap.of(), keyCoder, "");
  }

  /**
   * Instantiates a multi-language wrapper for a Python RunInference with a given model loader.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param schema A schema for output rows.
   * @return A {@link RunInference} for the given model loader.
   */
  public static RunInference<Row> of(String modelLoader, Schema schema) {
    return new RunInference<>(modelLoader, schema, ImmutableMap.of(), null, "");
  }

  /**
   * Similar to {@link RunInference#of(String, Schema)} but the input is a {@link PCollection} of
   * {@link KV}s.
   *
   * @param modelLoader A Python callable for a model loader class object.
   * @param schema A schema for output rows.
   * @param keyCoder a {@link Coder} for the input and output Key type.
   * @param <KeyT> input and output Key type. Inferred by the provided coder.
   * @return A {@link RunInference} for the given model loader.
   */
  public static <KeyT> RunInference<KV<KeyT, Row>> ofKVs(
      String modelLoader, Schema schema, Coder<KeyT> keyCoder) {
    return new RunInference<>(modelLoader, schema, ImmutableMap.of(), keyCoder, "");
  }

  /**
   * Sets keyword arguments for the model loader.
   *
   * @return A {@link RunInference} with keyword arguments.
   */
  public RunInference<OutputT> withKwarg(String key, Object arg) {
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder().putAll(kwargs).put(key, arg);
    return new RunInference<>(modelLoader, schema, builder.build(), keyCoder, expansionService);
  }

  /**
   * Specifies any extra packages required by the RunInference model handler.
   *
   * <p>This should only be specified when using the default expansion service, i.e. when not using
   * {@link #withExpansionService(String)} to provide an expansion service.
   *
   * <p>The package can either be a PyPi package or the path to a locally available Python package.
   *
   * <p>For model handlers provided by Beam Python SDK, the implementation will automatically try to
   * infer correct packages needed, so this may be omitted.
   *
   * @param extraPackages a list of PyPi packages. May include the version.
   * @return A {@link RunInference} with extra packages.
   */
  public RunInference<OutputT> withExtraPackages(List<String> extraPackages) {
    if (!this.extraPackages.isEmpty()) {
      throw new IllegalArgumentException("Extra packages were already specified");
    }
    this.extraPackages.addAll(extraPackages);
    return this;
  }

  /**
   * Sets an expansion service endpoint for RunInference.
   *
   * @param expansionService A URL for a Python expansion service.
   * @return A {@link RunInference} for the given expansion service endpoint.
   */
  public RunInference<OutputT> withExpansionService(String expansionService) {
    return new RunInference<>(modelLoader, schema, kwargs, keyCoder, expansionService);
  }

  private RunInference(
      String modelLoader,
      Schema schema,
      Map<String, Object> kwargs,
      @Nullable Coder<?> keyCoder,
      String expansionService) {
    this.modelLoader = modelLoader;
    this.schema = schema;
    this.kwargs = kwargs;
    this.keyCoder = keyCoder;
    this.expansionService = expansionService;
    this.extraPackages = new ArrayList<>();
  }

  private List<String> inferExtraPackagesFromModelHandler() {
    List<String> extraPackages = new ArrayList<>();
    if (this.modelLoader.toLowerCase().contains("sklearn")) {
      extraPackages.add("scikit-learn");
      extraPackages.add("pandas");
    } else if (this.modelLoader.toLowerCase().contains("pytorch")) {
      extraPackages.add("torch");
    }

    if (!extraPackages.isEmpty()) {
      LOG.info(
          "Automatically inferred dependencies {} from the provided model handler.", extraPackages);
    }

    return extraPackages;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<?> input) {
    Coder<OutputT> outputCoder;
    if (this.keyCoder == null) {
      outputCoder = (Coder<OutputT>) RowCoder.of(schema);
    } else {
      outputCoder = (Coder<OutputT>) KvCoder.of(keyCoder, RowCoder.of(schema));
    }

    if (this.expansionService.isEmpty() && this.extraPackages.isEmpty()) {
      this.extraPackages.addAll(inferExtraPackagesFromModelHandler());
    }

    return (PCollection<OutputT>)
        input.apply(
            PythonExternalTransform.<PCollection<?>, PCollection<Row>>from(
                    "apache_beam.ml.inference.base.RunInference.from_callable", expansionService)
                .withKwarg("model_handler_provider", PythonCallableSource.of(modelLoader))
                .withOutputCoder(outputCoder)
                .withExtraPackages(this.extraPackages)
                .withKwargs(kwargs));
  }
}
