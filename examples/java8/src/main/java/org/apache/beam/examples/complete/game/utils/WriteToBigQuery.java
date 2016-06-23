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
package org.apache.beam.examples.complete.game.utils;

import org.apache.beam.examples.complete.game.UserScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.io.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Generate, format, and write BigQuery table row information. Use provided information about
 * the field names and types, as well as lambda functions that describe how to generate their
 * values.
 */
public class WriteToBigQuery<T>
    extends PTransform<PCollection<T>, PDone> {

  protected String tableName;
  protected Map<String, FieldInfo<T>> fieldInfo;

  public WriteToBigQuery() {
  }

  public WriteToBigQuery(String tableName,
      Map<String, FieldInfo<T>> fieldInfo) {
    this.tableName = tableName;
    this.fieldInfo = fieldInfo;
  }

  /** Define a class to hold information about output table field definitions. */
  public static class FieldInfo<T> implements Serializable {
    // The BigQuery 'type' of the field
    private String fieldType;
    // A lambda function to generate the field value
    private SerializableFunction<DoFn<T, TableRow>.ProcessContext, Object> fieldFn;

    public FieldInfo(String fieldType,
        SerializableFunction<DoFn<T, TableRow>.ProcessContext, Object> fieldFn) {
      this.fieldType = fieldType;
      this.fieldFn = fieldFn;
    }

    String getFieldType() {
      return this.fieldType;
    }

    SerializableFunction<DoFn<T, TableRow>.ProcessContext, Object> getFieldFn() {
      return this.fieldFn;
    }
  }
  /** Convert each key/score pair into a BigQuery TableRow as specified by fieldFn. */
  protected class BuildRowFn extends DoFn<T, TableRow> {

    @Override
    public void processElement(ProcessContext c) {

      TableRow row = new TableRow();
      for (Map.Entry<String, FieldInfo<T>> entry : fieldInfo.entrySet()) {
          String key = entry.getKey();
          FieldInfo<T> fcnInfo = entry.getValue();
          SerializableFunction<DoFn<T, TableRow>.ProcessContext, Object> fcn =
            fcnInfo.getFieldFn();
          row.set(key, fcn.apply(c));
        }
      c.output(row);
    }
  }

  /** Build the output table schema. */
  protected TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    for (Map.Entry<String, FieldInfo<T>> entry : fieldInfo.entrySet()) {
      String key = entry.getKey();
      FieldInfo<T> fcnInfo = entry.getValue();
      String bqType = fcnInfo.getFieldType();
      fields.add(new TableFieldSchema().setName(key).setType(bqType));
    }
    return new TableSchema().setFields(fields);
  }

  @Override
  public PDone apply(PCollection<T> teamAndScore) {
    return teamAndScore
      .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
      .apply(BigQueryIO.Write
                .to(getTable(teamAndScore.getPipeline(),
                    tableName))
                .withSchema(getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
  }

  /** Utility to construct an output table reference. */
  static TableReference getTable(Pipeline pipeline, String tableName) {
    PipelineOptions options = pipeline.getOptions();
    TableReference table = new TableReference();
    table.setDatasetId(options.as(UserScore.Options.class).getDataset());
    table.setProjectId(options.as(GcpOptions.class).getProject());
    table.setTableId(tableName);
    return table;
  }
}
