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
package org.apache.beam.fn.harness.data;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * ElementsDispatcher is responsible to dispatch in memory stored Elements data to the corresponding
 * consumer.
 *
 * @param <T> Output data type.
 */
public class ElementsDataDispatcher<T> {
  private final Elements elements;
  private final Coder<WindowedValue<T>> coder;
  private final FnDataReceiver<WindowedValue<T>> dataConsumer;

  public ElementsDataDispatcher(
      Elements elements,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> dataConsumer) {
    this.elements = elements;
    this.coder = coder;
    this.dataConsumer = dataConsumer;
  }

  /**
   * Decodes and dispatches the elements to corresponding consumer. Should be invoked at the process
   * element stage of the read transform and after the downstream DoFn transforms have finished
   * StartBundle setup.
   *
   * @throws Exception when the Coder fails to decode the data, or the consumer fails to process the
   *     decoded data.
   */
  public void dispatch() throws Exception {
    if (this.elements == null || this.elements.getDataList() == null) {
      return;
    }
    for (Elements.Data data : this.elements.getDataList()) {
      // Runner probably won't send data that has last bit set if it is embedded in
      // ProcessBundleRequest, but to be safe we skip it if seen.
      if (data.getIsLast()) {
        continue;
      }
      WindowedValue<T> windowedValue = coder.decode(data.getData().newInput());

      this.dataConsumer.accept(windowedValue);
    }
  }
}
