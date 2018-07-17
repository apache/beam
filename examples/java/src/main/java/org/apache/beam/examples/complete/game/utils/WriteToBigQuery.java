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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Generate, format, and write BigQuery table row information. Use provided information about the
 * field names and types, as well as lambda functions that describe how to generate their values.
 */
public class WriteToBigQuery<InputT> extends PTransform<PCollection<InputT>, PDone> {

  protected String projectId;
  protected String datasetId;
  protected String tableName;
  protected Map<String, FieldInfo<InputT>> fieldInfo;

  public WriteToBigQuery() {}

  public WriteToBigQuery(
      String projectId,
      String datasetId,
      String tableName,
      Map<String, FieldInfo<InputT>> fieldInfo) {
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableName = tableName;
    this.fieldInfo = fieldInfo;
  }

  /**
   * A {@link Serializable} function from a {@link DoFn.ProcessContext} and {@link BoundedWindow} to
   * the value for that field.
   */
  public interface FieldFn<InputT> extends Serializable {
    Object apply(DoFn<InputT, TableRow>.ProcessContext context, BoundedWindow window);
  }

  /** Define a class to hold information about output table field definitions. */
  public static class FieldInfo<InputT> implements Serializable {
    // The BigQuery 'type' of the field
    private String fieldType;
    // A lambda function to generate the field value
    private FieldFn<InputT> fieldFn;

    public FieldInfo(String fieldType, FieldFn<InputT> fieldFn) {
      this.fieldType = fieldType;
      this.fieldFn = fieldFn;
    }

    String getFieldType() {
      return this.fieldType;
    }

    FieldFn<InputT> getFieldFn() {
      return this.fieldFn;
    }
  }

  /** Convert each key/score pair into a BigQuery TableRow as specified by fieldFn. */
  protected class BuildRowFn extends DoFn<InputT, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {

      TableRow row = new TableRow();
      for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
        String key = entry.getKey();
        FieldInfo<InputT> fcnInfo = entry.getValue();
        FieldFn<InputT> fcn = fcnInfo.getFieldFn();
        row.set(key, fcn.apply(c, window));
      }
      c.output(row);
    }
  }

  /** Build the output table schema. */
  protected TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
      String key = entry.getKey();
      FieldInfo<InputT> fcnInfo = entry.getValue();
      String bqType = fcnInfo.getFieldType();
      fields.add(new TableFieldSchema().setName(key).setType(bqType));
    }
    return new TableSchema().setFields(fields);
  }

  @Override
  public PDone expand(PCollection<InputT> teamAndScore) {
    teamAndScore
        .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(getTable(projectId, datasetId, tableName))
                .withSchema(getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    return PDone.in(teamAndScore.getPipeline());
  }

  /** Utility to construct an output table reference. */
  static TableReference getTable(String projectId, String datasetId, String tableName) {
    TableReference table = new TableReference();
    table.setDatasetId(datasetId);
    table.setProjectId(projectId);
    table.setTableId(tableName);
    return table;
  }
}
