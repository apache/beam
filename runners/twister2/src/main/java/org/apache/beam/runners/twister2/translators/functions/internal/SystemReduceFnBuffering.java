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
package org.apache.beam.runners.twister2.translators.functions.internal;

import java.io.ObjectStreamException;
import org.apache.beam.runners.core.MergingStateAccessor;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.runners.core.StateMerging;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;

public class SystemReduceFnBuffering<K, T, W extends BoundedWindow>
    extends SystemReduceFn<K, T, Iterable<T>, Iterable<T>, W> {
  private static final String BUFFER_NAME = "buf";
  private transient StateTag<BagState<T>> bufferTagLocal;
  private transient Coder<T> inputCoder;

  private transient boolean isInitialized = false;

  private byte[] coderBytes;

  public SystemReduceFnBuffering() {
    super(null);
    this.isInitialized = false;
  }

  public SystemReduceFnBuffering(Coder<T> valueCoder) {
    super(null);
    bufferTagLocal = null;
    coderBytes = SerializableUtils.serializeToByteArray(valueCoder);
    ;

    inputCoder = valueCoder;
  }

  @Override
  public void onMerge(OnMergeContext context) throws Exception {
    initTransient();
    StateMerging.mergeBags(context.state(), bufferTagLocal);
  }

  @Override
  public void prefetchOnMerge(MergingStateAccessor<K, W> state) throws Exception {
    initTransient();
    StateMerging.prefetchBags(state, bufferTagLocal);
  }

  @Override
  public void processValue(ProcessValueContext c) throws Exception {
    initTransient();
    c.state().access(bufferTagLocal).add(c.value());
  }

  @Override
  public void prefetchOnTrigger(StateAccessor<K> state) {
    initTransient();
    state.access(bufferTagLocal).readLater();
  }

  @Override
  public void onTrigger(OnTriggerContext c) throws Exception {
    initTransient();
    c.output(c.state().access(bufferTagLocal).read());
  }

  @Override
  public void clearState(Context c) throws Exception {
    initTransient();
    c.state().access(bufferTagLocal).clear();
  }

  @Override
  public ReadableState<Boolean> isEmpty(StateAccessor<K> state) {
    initTransient();
    return state.access(bufferTagLocal).isEmpty();
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }
    inputCoder =
        (Coder<T>) SerializableUtils.deserializeFromByteArray(coderBytes, "Custom Coder Bytes");
    bufferTagLocal = StateTags.makeSystemTagInternal(StateTags.bag(BUFFER_NAME, inputCoder));
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
