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

import java.util.Map;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelAdaptersMapping;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * Convert Java model object to Row.
 */
public class ToRow {

  static final ToRow INSTANCE = new ToRow(ModelAdaptersMapping.ADAPTERS);

  private Map<Class, ModelFieldsAdapter> modelTypeAdapters;

  private ToRow(Map<Class, ModelFieldsAdapter> modelTypeAdapters) {
    this.modelTypeAdapters = modelTypeAdapters;
  }

  private Row toRow(Event event) {
    if (event == null) {
      return null;
    }

    KnownSize model = getModel(event);
    Class modelClass = model.getClass();

    if (!modelTypeAdapters.containsKey(modelClass)) {
      throw new IllegalArgumentException(
          "Beam SQL record type adapter is not registered for " + model.getClass().getSimpleName());
    }

    ModelFieldsAdapter adapter = modelTypeAdapters.get(modelClass);

    return Row
        .withSchema(adapter.getSchema())
        .addValues(adapter.getFieldsValues(model))
        .build();
  }

  private KnownSize getModel(Event event) {
    if (event.newAuction != null) {
      return event.newAuction;
    } else if (event.newPerson != null) {
      return event.newPerson;
    } else if (event.bid != null) {
      return event.bid;
    }

    throw new IllegalStateException("Unsupported event type " + event);
  }

  public static ParDo.SingleOutput<Event, Row> parDo() {
    return ParDo.of(new DoFn<Event, Row>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        Row row = INSTANCE.toRow(c.element());
        c.output(row);
      }
    });
  }
}
