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
package org.apache.beam.runners.twister2.translators.functions;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import java.io.ObjectStreamException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.twister2.utils.TranslationUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;

/** ByteToWindow function. */
public class ByteToWindowFunction<K, V>
    implements MapFunc<KV<K, Iterable<WindowedValue<V>>>, Tuple<byte[], Iterator<byte[]>>> {
  private transient Coder<K> keyCoder;
  private transient WindowedValueCoder<V> wvCoder;
  private static final Logger LOG = Logger.getLogger(ByteToWindowFunction.class.getName());

  private transient boolean isInitialized = false;
  private byte[] keyCoderBytes;
  private byte[] wvCoderBytes;

  public ByteToWindowFunction() {
    // non arg constructor needed for kryo
    isInitialized = false;
  }

  public ByteToWindowFunction(final Coder<K> inputKeyCoder, final WindowedValueCoder<V> wvCoder) {
    this.keyCoder = inputKeyCoder;
    this.wvCoder = wvCoder;

    keyCoderBytes = SerializableUtils.serializeToByteArray(keyCoder);
    wvCoderBytes = SerializableUtils.serializeToByteArray(wvCoder);
  }

  @Override
  public KV<K, Iterable<WindowedValue<V>>> map(Tuple<byte[], Iterator<byte[]>> input) {
    K key = null;
    Iterable<WindowedValue<V>> value = null;
    try {
      key = CoderUtils.decodeFromByteArray(keyCoder, input.getKey());
      // TODO need to replace this with a custom iterator
      value =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(input.getValue(), Spliterator.ORDERED), false)
              .map(bytes -> TranslationUtils.fromByteArray(bytes, wvCoder))
              .collect(Collectors.toList());
    } catch (CoderException e) {
      LOG.info(e.getMessage());
    }
    return KV.of(key, value);
  }

  @Override
  public void prepare(TSetContext context) {
    initTransient();
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }

    keyCoder =
        (Coder<K>) SerializableUtils.deserializeFromByteArray(keyCoderBytes, "Custom Coder Bytes");
    wvCoder =
        (WindowedValueCoder<V>)
            SerializableUtils.deserializeFromByteArray(wvCoderBytes, "Custom Coder Bytes");
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
