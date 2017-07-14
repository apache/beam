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
package org.apache.beam.runners.jstorm.translation;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Translates a {@link Flatten} to a JStorm {@link FlattenExecutor}.
 * @param <V>
 */
class FlattenTranslator<V> extends TransformTranslator.Default<Flatten.PCollections<V>> {

  @Override
  public void translateNode(Flatten.PCollections<V> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();

    // Since a new tag is created in PCollectionList, retrieve the real tag here.
    Map<TupleTag<?>, PValue> inputs = Maps.newHashMap();
    for (Map.Entry<TupleTag<?>, PValue> entry : userGraphContext.getInputs().entrySet()) {
      PCollection<V> pc = (PCollection<V>) entry.getValue();
      inputs.putAll(pc.expand());
    }
    System.out.println("Real inputs: " + inputs);
    System.out.println("FlattenList inputs: " + userGraphContext.getInputs());
    String description = describeTransform(transform, inputs, userGraphContext.getOutputs());
    FlattenExecutor executor = new FlattenExecutor(description, userGraphContext.getOutputTag());
    context.addTransformExecutor(executor, inputs, userGraphContext.getOutputs());
  }
}
