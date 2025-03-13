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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

/**
 * Prepare an input {@link PCollection} for writing to BigQuery. Use the table function to determine
 * which tables each element is written to, and format the element into a {@link TableRow} using the
 * user-supplied format function.
 */
public class PrepareWrite<InputT, DestinationT extends @NonNull Object, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<KV<DestinationT, OutputT>>> {
  private DynamicDestinations<InputT, DestinationT> dynamicDestinations;
  private SerializableFunction<InputT, OutputT> formatFunction;

  public PrepareWrite(
      DynamicDestinations<InputT, DestinationT> dynamicDestinations,
      SerializableFunction<InputT, OutputT> formatFunction) {
    this.dynamicDestinations = dynamicDestinations;
    this.formatFunction = formatFunction;
  }

  @Override
  public PCollection<KV<DestinationT, OutputT>> expand(PCollection<InputT> input) {
    return input.apply(
        ParDo.of(
                new DoFn<InputT, KV<DestinationT, OutputT>>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext context,
                      @Element InputT element,
                      @Timestamp Instant timestamp,
                      BoundedWindow window,
                      PaneInfo pane)
                      throws IOException {
                    dynamicDestinations.setSideInputAccessorFromProcessContext(context);
                    ValueInSingleWindow<InputT> windowedElement =
                        ValueInSingleWindow.of(element, timestamp, window, pane);
                    DestinationT tableDestination =
                        dynamicDestinations.getDestination(windowedElement);
                    checkArgument(
                        tableDestination != null,
                        "DynamicDestinations.getDestination() may not return null, "
                            + "but %s returned null on element %s",
                        dynamicDestinations,
                        element);
                    OutputT outputValue = formatFunction.apply(element);
                    Preconditions.checkArgumentNotNull(
                        outputValue,
                        "formatFunction may not return null, but %s returned null on element %s",
                        formatFunction,
                        element);
                    context.output(KV.of(tableDestination, outputValue));
                  }
                })
            .withSideInputs(dynamicDestinations.getSideInputs()));
  }
}
