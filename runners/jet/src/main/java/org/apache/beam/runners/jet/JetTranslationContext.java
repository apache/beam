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
package org.apache.beam.runners.jet;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

class JetTranslationContext {

  private final SerializablePipelineOptions options;
  private final DAGBuilder dagBuilder;

  JetTranslationContext(JetPipelineOptions options) {
    this.options = new SerializablePipelineOptions(options);
    this.dagBuilder = new DAGBuilder(options);
  }

  SerializablePipelineOptions getOptions() {
    return options;
  }

  DAGBuilder getDagBuilder() {
    return dagBuilder;
  }

  <T> WindowedValue.FullWindowedValueCoder<T> getTypeInfo(PCollection<T> collection) {
    return getTypeInfo(collection.getCoder(), collection.getWindowingStrategy());
  }

  <T> WindowedValue.FullWindowedValueCoder<T> getTypeInfo(
      Coder<T> coder, WindowingStrategy<?, ?> windowingStrategy) {
    return WindowedValue.getFullCoder(coder, windowingStrategy.getWindowFn().windowCoder());
  }
}
