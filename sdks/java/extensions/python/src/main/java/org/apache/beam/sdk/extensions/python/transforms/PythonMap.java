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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Wrapper for invoking external Python {@code Map} transforms.. @Experimental */
public class PythonMap<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {

  private PythonCallableSource pythonFunction;
  private @Nullable String expansionService;
  private Coder<?> outputCoder;
  private static final String PYTHON_MAP_FN_TRANSFORM = "apache_beam.Map";
  private static final String PYTHON_FLATMAP_FN_TRANSFORM = "apache_beam.FlatMap";
  private String pythonTransform;

  private PythonMap(
      PythonCallableSource pythonFunction, Coder<?> outputCoder, String pythonTransform) {
    this.pythonFunction = pythonFunction;
    this.outputCoder = outputCoder;
    this.pythonTransform = pythonTransform;
  }

  public static <InputT, OutputT> PythonMap<InputT, OutputT> viaMapFn(
      String pythonFunction, Coder<?> outputCoder) {
    return new PythonMap<>(
        PythonCallableSource.of(pythonFunction), outputCoder, PYTHON_MAP_FN_TRANSFORM);
  }

  public static <InputT, OutputT> PythonMap<InputT, OutputT> viaFlatMapFn(
      String pythonFunction, Coder<?> outputCoder) {
    return new PythonMap<>(
        PythonCallableSource.of(pythonFunction), outputCoder, PYTHON_FLATMAP_FN_TRANSFORM);
  }

  public PythonMap<InputT, OutputT> withExpansionService(String expansionService) {
    this.expansionService = expansionService;
    return this;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    expansionService = (expansionService != null) ? expansionService : "";
    PythonExternalTransform<PCollection<? extends InputT>, PCollection<OutputT>> pythonMapElements =
        PythonExternalTransform.from(pythonTransform, expansionService);
    pythonMapElements.withArgs(pythonFunction);
    pythonMapElements.withOutputCoder(outputCoder);
    return input.apply(pythonMapElements);
  }
}
