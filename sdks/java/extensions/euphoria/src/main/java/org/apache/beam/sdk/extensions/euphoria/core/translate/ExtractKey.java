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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Key extracting utility transformation shared among operator translators. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ExtractKey<KeyT, ValueT>
    extends PTransform<PCollection<ValueT>, PCollection<KV<KeyT, ValueT>>> {

  private static class ExtractKeyFn<KeyT, InputT> extends DoFn<InputT, KV<KeyT, InputT>> {

    private final UnaryFunction<InputT, KeyT> keyExtractor;

    ExtractKeyFn(UnaryFunction<InputT, KeyT> keyExtractor) {
      this.keyExtractor = keyExtractor;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext c) {
      final InputT element = c.element();
      final KeyT key = keyExtractor.apply(element);
      c.output(KV.of(key, element));
    }
  }

  private final UnaryFunction<ValueT, KeyT> keyExtractor;
  private final TypeDescriptor<KeyT> keyType;

  ExtractKey(UnaryFunction<ValueT, KeyT> keyExtractor, TypeDescriptor<KeyT> keyType) {
    this.keyExtractor = requireNonNull(keyExtractor);
    this.keyType = requireNonNull(keyType);
  }

  @Override
  public PCollection<KV<KeyT, ValueT>> expand(PCollection<ValueT> input) {
    Objects.requireNonNull(input.getTypeDescriptor());
    return input
        .apply(ParDo.of(new ExtractKeyFn<>(keyExtractor)))
        .setTypeDescriptor(TypeDescriptors.kvs(keyType, input.getTypeDescriptor()));
  }
}
