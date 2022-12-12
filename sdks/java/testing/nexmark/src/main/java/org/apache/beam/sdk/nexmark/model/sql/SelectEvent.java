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
package org.apache.beam.sdk.nexmark.model.sql;

import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Select a person bid or auction out of Java Event and return it as a row. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SelectEvent extends PTransform<PCollection<Event>, PCollection<Row>> {
  private Event.Type eventType;

  public SelectEvent(Event.Type eventType) {
    this.eventType = eventType;
  }

  int getNestedIndex(Schema schema) {
    switch (eventType) {
      case PERSON:
        return schema.indexOf("newPerson");
      case AUCTION:
        return schema.indexOf("newAuction");
      case BID:
        return schema.indexOf("bid");
      default:
        throw new RuntimeException("Unexpected event type.");
    }
  }

  @Override
  public PCollection<Row> expand(PCollection<Event> input) {
    if (!input.hasSchema()) {
      throw new RuntimeException("Input PCollection must have a schema!");
    }
    int index = getNestedIndex(input.getSchema());
    return input
        .apply(
            ParDo.of(
                new DoFn<Event, Row>() {
                  @ProcessElement
                  public void processElement(@Element Row row, OutputReceiver<Row> o) {
                    o.output(row.getRow(index));
                  }
                }))
        .setRowSchema(input.getSchema().getField(index).getType().getRowSchema());
  }
}
