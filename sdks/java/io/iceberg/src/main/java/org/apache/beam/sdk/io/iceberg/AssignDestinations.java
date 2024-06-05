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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Assigns the destination metadata for each input record.
 *
 * <p>The output record will have the format { dest: ..., data: ...} where the dest field has the
 * assigned metadata and the data field has the original row.
 */
class AssignDestinations extends PTransform<PCollection<Row>, PCollection<Row>> {

  private DynamicDestinations dynamicDestinations;

  public AssignDestinations(DynamicDestinations dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    final Schema inputSchema = input.getSchema();
    final Schema outputSchema =
        Schema.builder()
            .addRowField("data", inputSchema)
            .addRowField("dest", dynamicDestinations.getMetadataSchema())
            .build();

    return input
        .apply(
            ParDo.of(
                new DoFn<Row, Row>() {
                  @ProcessElement
                  public void processElement(@Element Row data, OutputReceiver<Row> out) {
                    out.output(
                        Row.withSchema(outputSchema)
                            .addValues(data, dynamicDestinations.assignDestinationMetadata(data))
                            .build());
                  }
                }))
        .setRowSchema(outputSchema);
  }
}
