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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.TimerInternals.TimerDataCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/**
 * A {@link Coder} for {@link KeyedWorkItem KeyedWorkItems}.
 */
public class KeyedWorkItemCoder<K, ElemT> extends StandardCoder<KeyedWorkItem<K, ElemT>> {
  /**
   * Create a new {@link KeyedWorkItemCoder} with the provided key coder, element coder, and window
   * coder.
   */
  public static <K, ElemT> KeyedWorkItemCoder<K, ElemT> of(
      Coder<K> keyCoder, Coder<ElemT> elemCoder, Coder<? extends BoundedWindow> windowCoder) {
    return new KeyedWorkItemCoder<>(keyCoder, elemCoder, windowCoder);
  }

  @JsonCreator
  public static <K, ElemT> KeyedWorkItemCoder<K, ElemT> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
    checkArgument(components.size() == 3, "Expecting 3 components, got %s", components.size());
    @SuppressWarnings("unchecked")
    Coder<K> keyCoder = (Coder<K>) components.get(0);
    @SuppressWarnings("unchecked")
    Coder<ElemT> elemCoder = (Coder<ElemT>) components.get(1);
    @SuppressWarnings("unchecked")
    Coder<? extends BoundedWindow> windowCoder = (Coder<? extends BoundedWindow>) components.get(2);
    return new KeyedWorkItemCoder<>(keyCoder, elemCoder, windowCoder);
  }

  private final Coder<K> keyCoder;
  private final Coder<ElemT> elemCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final Coder<Iterable<TimerData>> timersCoder;
  private final Coder<Iterable<WindowedValue<ElemT>>> elemsCoder;

  private KeyedWorkItemCoder(
      Coder<K> keyCoder, Coder<ElemT> elemCoder, Coder<? extends BoundedWindow> windowCoder) {
    this.keyCoder = keyCoder;
    this.elemCoder = elemCoder;
    this.windowCoder = windowCoder;
    this.timersCoder = IterableCoder.of(TimerDataCoder.of(windowCoder));
    this.elemsCoder = IterableCoder.of(FullWindowedValueCoder.of(elemCoder, windowCoder));
  }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public Coder<ElemT> getElementCoder() {
    return elemCoder;
  }

  @Override
  public void encode(KeyedWorkItem<K, ElemT> value, OutputStream outStream, Coder.Context context)
      throws CoderException, IOException {
    Coder.Context nestedContext = context.nested();
    keyCoder.encode(value.key(), outStream, nestedContext);
    timersCoder.encode(value.timersIterable(), outStream, nestedContext);
    elemsCoder.encode(value.elementsIterable(), outStream, nestedContext);
  }

  @Override
  public KeyedWorkItem<K, ElemT> decode(InputStream inStream, Coder.Context context)
      throws CoderException, IOException {
    Coder.Context nestedContext = context.nested();
    K key = keyCoder.decode(inStream, nestedContext);
    Iterable<TimerData> timers = timersCoder.decode(inStream, nestedContext);
    Iterable<WindowedValue<ElemT>> elems = elemsCoder.decode(inStream, nestedContext);
    return KeyedWorkItems.workItem(key, timers, elems);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(keyCoder, elemCoder, windowCoder);
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {
    keyCoder.verifyDeterministic();
    timersCoder.verifyDeterministic();
    elemsCoder.verifyDeterministic();
  }

  /**
   * {@inheritDoc}.
   *
   * {@link KeyedWorkItemCoder} is not consistent with equals as it can return a
   * {@link KeyedWorkItem} of a type different from the originally encoded type.
   */
  @Override
  public boolean consistentWithEquals() {
    return false;
  }
}
