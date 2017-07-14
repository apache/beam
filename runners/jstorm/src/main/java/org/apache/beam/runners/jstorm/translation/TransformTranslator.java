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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Interface for classes capable of tranforming Beam PTransforms into Storm primitives.
 */
interface TransformTranslator<T extends PTransform<?, ?>> {

  void translateNode(T transform, TranslationContext context);

  /**
   * Returns true if this translator can translate the given transform.
   */
  boolean canTranslate(T transform, TranslationContext context);

    /**
     * Default translator.
     * @param <T1>
     */
  class Default<T1 extends PTransform<?, ?>> implements TransformTranslator<T1> {
    @Override
    public void translateNode(T1 transform, TranslationContext context) {

    }

    @Override
    public boolean canTranslate(T1 transform, TranslationContext context) {
      return true;
    }

    static String describeTransform(
        PTransform<?, ?> transform,
        Map<TupleTag<?>, PValue> inputs,
        Map<TupleTag<?>, PValue> outputs) {
      return String.format("%s --> %s --> %s",
          Joiner.on('+').join(FluentIterable.from(inputs.entrySet())
              .transform(new Function<Map.Entry<TupleTag<?>, PValue>, String>() {
                @Override
                public String apply(Map.Entry<TupleTag<?>, PValue> taggedPValue) {
                  return taggedPValue.getKey().getId();
                }
              })),
          transform.getName(),
          Joiner.on('+').join(FluentIterable.from(outputs.entrySet())
              .transform(new Function<Map.Entry<TupleTag<?>, PValue>, String>() {
                @Override
                public String apply(Map.Entry<TupleTag<?>, PValue> taggedPvalue) {
                  return taggedPvalue.getKey().getId();
                }
              })));
    }
  }
}
