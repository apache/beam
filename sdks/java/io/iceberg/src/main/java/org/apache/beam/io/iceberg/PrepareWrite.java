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
package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
public class PrepareWrite<InputT, DestinationT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<KV<DestinationT, OutputT>>> {

  private DynamicDestinations<InputT, DestinationT> dynamicDestinations;
  private SerializableFunction<InputT, OutputT> formatFunction;
  private Coder outputCoder;

  public PrepareWrite(
      DynamicDestinations<InputT, DestinationT> dynamicDestinations,
      SerializableFunction<InputT, OutputT> formatFunction,
      Coder outputCoder) {
    this.dynamicDestinations = dynamicDestinations;
    this.formatFunction = formatFunction;
    this.outputCoder = outputCoder;
  }

  @Override
  public PCollection<KV<DestinationT, OutputT>> expand(PCollection<InputT> input) {

    final Coder destCoder;
    try {
      destCoder =
          KvCoder.of(
              dynamicDestinations.getDestinationCoderWithDefault(
                  input.getPipeline().getCoderRegistry()),
              outputCoder);
    } catch (Exception e) {
      RuntimeException e1 = new RuntimeException("Unable to expand PrepareWrite");
      e1.addSuppressed(e);
      throw e1;
    }
    return input
        .apply(
            ParDo.of(
                    new DoFn<InputT, KV<DestinationT, OutputT>>() {

                      @ProcessElement
                      public void processElement(
                          ProcessContext c,
                          @Element InputT element,
                          @Timestamp Instant timestamp,
                          BoundedWindow window,
                          PaneInfo pane)
                          throws IOException {
                        ValueInSingleWindow<InputT> windowedElement =
                            ValueInSingleWindow.of(element, timestamp, window, pane);
                        dynamicDestinations.setSideInputProcessContext(c);
                        DestinationT tableDestination =
                            dynamicDestinations.getDestination(windowedElement);
                        OutputT outputValue = formatFunction.apply(element);
                        c.output(KV.of(tableDestination, outputValue));
                      }
                    })
                .withSideInputs(dynamicDestinations.getSideInputs()))
        .setCoder(destCoder);
  }
}
