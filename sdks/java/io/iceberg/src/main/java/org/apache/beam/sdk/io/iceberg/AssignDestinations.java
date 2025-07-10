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
package org.apache.beam.sdk.io.iceberg;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

/**
 * Assigns the destination metadata for each input record.
 *
 * <p>The output record will have the format { dest: ..., data: ...} where the dest field has the
 * assigned metadata and the data field has the original row.
 */
class AssignDestinations extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {

  private final DynamicDestinations dynamicDestinations;

  public AssignDestinations(DynamicDestinations dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollection<KV<String, Row>> expand(PCollection<Row> input) {
    return input
        .apply(
            ParDo.of(
                new DoFn<Row, KV<String, Row>>() {
                  @ProcessElement
                  public void processElement(
                      @Element Row element,
                      BoundedWindow window,
                      PaneInfo paneInfo,
                      @Timestamp Instant timestamp,
                      OutputReceiver<KV<String, Row>> out) {
                    String tableIdentifier =
                        dynamicDestinations.getTableStringIdentifier(
                            ValueInSingleWindow.of(element, timestamp, window, paneInfo));
                    Row data = dynamicDestinations.getData(element);

                    out.output(KV.of(tableIdentifier, data));
                  }
                }))
        .setCoder(
            KvCoder.of(StringUtf8Coder.of(), RowCoder.of(dynamicDestinations.getDataSchema())));
  }
}
