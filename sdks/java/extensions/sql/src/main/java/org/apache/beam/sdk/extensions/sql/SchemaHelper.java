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

package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.reflect.InferredRowCoder;

/** Utility methods to help wire up schema inferring using {@link InferredRowCoder}. */
class SchemaHelper {

  static PCollection<Row> toRows(PInput input) {
    PCollection<?> pCollection = (PCollection<?>) input;
    Coder coder = pCollection.getCoder();

    if (coder instanceof RowCoder) {
      return (PCollection<Row>) pCollection;
    }

    if (coder instanceof InferredRowCoder) {
      InferredRowCoder inferredSchemaCoder = (InferredRowCoder) coder;
      return pCollection
          .apply(pCollection.getName() + "_transformToRows", transformToRows(inferredSchemaCoder))
          .setCoder(inferredSchemaCoder.rowCoder());
    }

    throw new UnsupportedOperationException(
        "Input PCollections for Beam SQL should either "
            + "have RowCoder set and contain Rows or "
            + "have InferredRowCoder for its elements");
  }

  private static PTransform<PCollection<?>, PCollection<Row>> transformToRows(
      InferredRowCoder coder) {

    return ParDo.of(
        new DoFn<Object, Row>() {
          @ProcessElement
          public void processElement(DoFn<Object, Row>.ProcessContext c) {
            c.output(coder.createRow(c.element()));
          }
        });
  }
}
