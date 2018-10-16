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
package org.apache.beam.sdk.extensions.euphoria.core.client.lib;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link PTransform} which allows you to build composite transformations in Euphoria API.
 *
 * @param <InputT> type of input elements
 * @param <OutputT> type of output elements
 */
public class Euphoria<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  private final Function<Dataset<InputT>, Dataset<OutputT>> fn;

  private Euphoria(Function<Dataset<InputT>, Dataset<OutputT>> fn) {
    this.fn = fn;
  }

  public static <InputT, OutputT> Euphoria<InputT, OutputT> of(
      Function<Dataset<InputT>, Dataset<OutputT>> fn) {
    return new Euphoria<>(requireNonNull(fn));
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> input) {
    return fn.apply(Dataset.of(input)).getPCollection();
  }
}
