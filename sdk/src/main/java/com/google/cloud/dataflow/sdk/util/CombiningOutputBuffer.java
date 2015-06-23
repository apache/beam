/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An {@link OutputBuffer} that uses a {@link KeyedCombineFn} to combine multiple inputs, allowing
 * it to store a single {@code AccumT} rather than all of the {@code InputT} values.
 */
class CombiningOutputBuffer<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    implements OutputBuffer<K, InputT, OutputT, W> {

  private static final long serialVersionUID = 0L;

  private final KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn;
  private final CodedTupleTag<AccumT> accumTag;

  private final Map<W, AccumT> inMemoryBuffer = new HashMap<>();

  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
  CombiningOutputBuffer<K, InputT, AccumT, OutputT, W> create(
      KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn,
      Coder<K> keyCoder,
      Coder<InputT> inputCoder) {
    CoderRegistry coderRegistry = new CoderRegistry();
    coderRegistry.registerStandardCoders();
    try {
      Coder<AccumT> accumCoder = combineFn.getAccumulatorCoder(coderRegistry, keyCoder, inputCoder);
      return new CombiningOutputBuffer<>(BUFFER_NAME, combineFn, accumCoder);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to determine coder for accumulator", e);
    }
  }

  public CombiningOutputBuffer(String bufferName,
      KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn,
      Coder<AccumT> accumCoder) {
    this.combineFn = combineFn;
    this.accumTag = CodedTupleTag.of(bufferName, accumCoder);
  }

  @Override
  public void addValue(OutputBuffer.Context<K, W> c, InputT input) throws IOException {
    // Only write to the in-memory accumulator.
    AccumT accum = inMemoryBuffer.get(c.window());
    if (accum == null) {
      accum = combineFn.createAccumulator(c.key());
    }
    inMemoryBuffer.put(c.window(), combineFn.addInput(c.key(), accum, input));
  }

  @Override
  public OutputT extract(OutputBuffer.Context<K, W> c) throws IOException {
    Iterable<AccumT> accums = FluentIterable.from(c.sourceWindows())
        .transform(Functions.forMap(inMemoryBuffer, null))
        .filter(Predicates.notNull())
        .append(c.readBuffers(accumTag, c.sourceWindows()));

    AccumT result = Iterables.isEmpty(accums)
        ? null : combineFn.mergeAccumulators(c.key(), accums);

    clear(c);
    inMemoryBuffer.put(c.window(), result);

    return result == null ? null : combineFn.extractOutput(c.key(), result);
  }

  private void clearSourceWindows(OutputBuffer.Context<K, W> c) throws IOException {
    c.clearBuffers(accumTag, c.sourceWindows());
    for (W window : c.sourceWindows()) {
      inMemoryBuffer.remove(window);
    }
  }

  @Override
  public void clear(OutputBuffer.Context<K, W> c) throws IOException {
    clearSourceWindows(c);
  }

  @Override
  public void flush(OutputBuffer.Context<K, W> c) throws IOException {
    for (Map.Entry<W, AccumT> entry : inMemoryBuffer.entrySet()) {
      c.addToBuffer(entry.getKey(), accumTag, entry.getValue());
    }
    inMemoryBuffer.clear();
  }
}
