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
package org.apache.beam.sdk.io.gcp.bigquery;

import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A transform that converts messages to protocol buffers in preparation for writing to BigQuery.
 */
public class StorageApiConvertMessages<DestinationT, ElementT>
    extends PTransform<
        PCollection<KV<DestinationT, ElementT>>, PCollection<KV<DestinationT, byte[]>>> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;

  public StorageApiConvertMessages(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollection<KV<DestinationT, byte[]>> expand(
      PCollection<KV<DestinationT, ElementT>> input) {
    String operationName = input.getName() + "/" + getName();

    return input.apply(
        "Convert to message",
        ParDo.of(new ConvertMessagesDoFn<>(dynamicDestinations, operationName))
            .withSideInputs(dynamicDestinations.getSideInputs()));
  }

  public static class ConvertMessagesDoFn<DestinationT, ElementT>
      extends DoFn<KV<DestinationT, ElementT>, KV<DestinationT, byte[]>> {
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;

    ConvertMessagesDoFn(
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        String operationName) {
      this.dynamicDestinations = dynamicDestinations;
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<DestinationT, ElementT> element,
        OutputReceiver<KV<DestinationT, byte[]>> o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      MessageConverter<ElementT> messageConverter =
          messageConverters.get(element.getKey(), dynamicDestinations);
      o.output(
          KV.of(element.getKey(), messageConverter.toMessage(element.getValue()).toByteArray()));
    }
  }
}
