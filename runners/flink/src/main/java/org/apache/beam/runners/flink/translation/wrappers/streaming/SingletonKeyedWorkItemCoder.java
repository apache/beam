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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Singleton keyed work item coder.
 */
public class SingletonKeyedWorkItemCoder<K, ElemT>
    extends StandardCoder<SingletonKeyedWorkItem<K, ElemT>> {
  /**
   * Create a new {@link KeyedWorkItemCoder} with the provided key coder, element coder, and window
   * coder.
   */
  public static <K, ElemT> SingletonKeyedWorkItemCoder<K, ElemT> of(
      Coder<K> keyCoder, Coder<ElemT> elemCoder, Coder<? extends BoundedWindow> windowCoder) {
    return new SingletonKeyedWorkItemCoder<>(keyCoder, elemCoder, windowCoder);
  }

  @JsonCreator
  public static <K, ElemT> SingletonKeyedWorkItemCoder<K, ElemT> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
    checkArgument(components.size() == 3, "Expecting 3 components, got %s", components.size());
    @SuppressWarnings("unchecked")
    Coder<K> keyCoder = (Coder<K>) components.get(0);
    @SuppressWarnings("unchecked")
    Coder<ElemT> elemCoder = (Coder<ElemT>) components.get(1);
    @SuppressWarnings("unchecked")
    Coder<? extends BoundedWindow> windowCoder = (Coder<? extends BoundedWindow>) components.get(2);
    return new SingletonKeyedWorkItemCoder<>(keyCoder, elemCoder, windowCoder);
  }

  private final Coder<K> keyCoder;
  private final Coder<ElemT> elemCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final WindowedValue.FullWindowedValueCoder<ElemT> valueCoder;

  private SingletonKeyedWorkItemCoder(
      Coder<K> keyCoder, Coder<ElemT> elemCoder, Coder<? extends BoundedWindow> windowCoder) {
    this.keyCoder = keyCoder;
    this.elemCoder = elemCoder;
    this.windowCoder = windowCoder;
    valueCoder = WindowedValue.FullWindowedValueCoder.of(elemCoder, windowCoder);
  }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public Coder<ElemT> getElementCoder() {
    return elemCoder;
  }

  @Override
  public void encode(SingletonKeyedWorkItem<K, ElemT> value,
                     OutputStream outStream,
                     Context context)
      throws CoderException, IOException {
    keyCoder.encode(value.key(), outStream, context.nested());
    valueCoder.encode(value.value, outStream, context);
  }

  @Override
  public SingletonKeyedWorkItem<K, ElemT> decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    K key = keyCoder.decode(inStream, context.nested());
    WindowedValue<ElemT> value = valueCoder.decode(inStream, context);
    return new SingletonKeyedWorkItem<>(key, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(keyCoder, elemCoder, windowCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    keyCoder.verifyDeterministic();
    elemCoder.verifyDeterministic();
    windowCoder.verifyDeterministic();
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
