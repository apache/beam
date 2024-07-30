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
package org.apache.beam.sdk.io.csv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * {@link CsvIORecordToObjects} is a class that takes an input of {@link PCollection<List<String>>}
 * and outputs custom type {@link PCollection<T>}.
 */
class CsvIORecordToObjects<T> extends PTransform<PCollection<List<String>>, PCollection<T>> {

  /** The expected {@link Schema} of the target type. */
  private final Schema schema;

  /** A map of the {@link Schema.Field#getName()} to the custom CSV processing lambda. */
  private final Map<String, SerializableFunction<String, Object>> customProcessingMap;

  /** A {@link Map} of {@link Schema.Field}s to their expected positions within the CSV record. */
  private final Map<Integer, Schema.Field> indexToFieldMap;

  /**
   * A {@link SerializableFunction} that converts from {@link Row} to {@link Schema} mapped custom
   * type.
   */
  private final SerializableFunction<Row, T> fromRowFn;

  /** The expected coder of target type. */
  private final Coder<T> coder;

  CsvIORecordToObjects(CsvIOParseConfiguration<T> configuration) {
    this.schema = configuration.getSchema();
    this.customProcessingMap = configuration.getCustomProcessingMap();
    this.indexToFieldMap =
        CsvIOParseHelpers.mapFieldPositions(configuration.getCsvFormat(), schema);
    this.fromRowFn = configuration.getFromRowFn();
    this.coder = configuration.getCoder();
  }

  @Override
  public PCollection<T> expand(PCollection<List<String>> input) {
    return input.apply(ParDo.of(new RecordToObjectsFn())).setCoder(coder);
  }

  private class RecordToObjectsFn extends DoFn<List<String>, T> {
    @ProcessElement
    public void process(@Element List<String> record, OutputReceiver<T> receiver) {
      Map<String, Object> fieldNamesToValues = new HashMap<>();
      for (Map.Entry<Integer, Schema.Field> entry : indexToFieldMap.entrySet()) {
        Schema.Field field = entry.getValue();
        int index = entry.getKey();
        String cell = record.get(index);
        Object value = parseCell(cell, field);
        fieldNamesToValues.put(field.getName(), value);
      }
      Row row = Row.withSchema(schema).withFieldValues(fieldNamesToValues).build();
      receiver.output(fromRowFn.apply(row));
    }
  }

  /** Parses cell to emit the value, as well as potential errors with filename. */
  Object parseCell(String cell, Schema.Field field) {
    if (cell == null) {
      if (!field.getType().getNullable()) {
        throw new IllegalArgumentException(
            "Required org.apache.beam.sdk.schemas.Schema field "
                + field.getName()
                + " has null value");
      }
      return cell;
    }
    if (customProcessingMap.containsKey(field.getName())) {
      return customProcessingMap.get(field.getName()).apply(cell);
    }
    return CsvIOParseHelpers.parseCell(cell, field);
  }
}
