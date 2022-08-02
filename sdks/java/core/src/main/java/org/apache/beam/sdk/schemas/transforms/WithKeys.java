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
package org.apache.beam.sdk.schemas.transforms;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class WithKeys<T> extends PTransform<PCollection<T>, PCollection<KV<Row, T>>> {
  private final FieldAccessDescriptor fieldAccessDescriptor;

  public static <T> WithKeys<T> of(FieldAccessDescriptor fieldAccessDescriptor) {
    return new WithKeys<>(fieldAccessDescriptor);
  }

  private WithKeys(FieldAccessDescriptor fieldAccessDescriptor) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
  }

  @Override
  public PCollection<KV<Row, T>> expand(PCollection<T> input) {
    Schema schema = input.getSchema();
    TypeDescriptor<T> typeDescriptor = input.getTypeDescriptor();
    if (typeDescriptor == null) {
      throw new RuntimeException("Null type descriptor on input.");
    }
    SerializableFunction<T, Row> toRowFunction = input.getToRowFunction();
    SerializableFunction<Row, T> fromRowFunction = input.getFromRowFunction();

    FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);
    RowSelector rowSelector = new RowSelectorContainer(schema, resolved, true);
    Schema keySchema = SelectHelpers.getOutputSchema(schema, resolved);

    return input
        .apply(
            "selectKeys",
            ParDo.of(
                new DoFn<T, KV<Row, T>>() {
                  @ProcessElement
                  public void process(
                      @Element Row row, // Beam will convert the element to a row.
                      @Element T element, // Beam will return the original element.
                      OutputReceiver<KV<Row, T>> o) {
                    o.output(KV.of(rowSelector.select(row), element));
                  }
                }))
        .setCoder(
            KvCoder.of(
                SchemaCoder.of(keySchema),
                SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction)));
  }
}
