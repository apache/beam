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

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import java.io.ObjectStreamException;
import org.apache.beam.runners.twister2.utils.TranslationUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;

/** ByteToWindow function. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ByteToElemFunction<V> implements MapFunc<WindowedValue<V>, byte[]> {
  private transient WindowedValueCoder<V> wvCoder;

  private transient boolean isInitialized = false;
  private byte[] wvCoderBytes;

  public ByteToElemFunction() {
    // non arg constructor needed for kryo
    isInitialized = false;
  }

  public ByteToElemFunction(final WindowedValueCoder<V> wvCoder) {
    this.wvCoder = wvCoder;

    wvCoderBytes = SerializableUtils.serializeToByteArray(wvCoder);
  }

  @Override
  public WindowedValue<V> map(byte[] input) {
    return TranslationUtils.fromByteArray(input, wvCoder);
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

    wvCoder =
        (WindowedValueCoder<V>)
            SerializableUtils.deserializeFromByteArray(wvCoderBytes, "Custom Coder Bytes");
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
