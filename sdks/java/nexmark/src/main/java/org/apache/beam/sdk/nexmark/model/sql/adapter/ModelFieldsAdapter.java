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

package org.apache.beam.sdk.nexmark.model.sql.adapter;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Schema;

/**
 * Helper class to help map Java model fields to {@link Schema} fields.
 */
public abstract class ModelFieldsAdapter<T> implements Serializable {

  private Schema schema;

  ModelFieldsAdapter(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  public abstract List<Object> getFieldsValues(T model);

  public abstract T getRowModel(Row row);

  public ParDo.SingleOutput<Row, T> parDo() {
    return ParDo.of(new DoFn<Row, T>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(getRowModel(c.element()));
        }
      });
  }
}
