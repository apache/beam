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

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.api.services.bigquery.model.TableRow;

import java.util.Map;

/**
 * Generate, format, and write BigQuery table row information. Subclasses {@link WriteToBigQuery}
 * to require windowing; so this subclass may be used for writes that require access to the
 * context's window information.
 */
public class WriteWindowedToBigQuery<T>
    extends WriteToBigQuery<T> {

  public WriteWindowedToBigQuery(String tableName,
      Map<String, FieldInfo<T>> fieldInfo) {
    super(tableName, fieldInfo);
  }

  /** Convert each key/score pair into a BigQuery TableRow. */
  protected class BuildRowFn extends OldDoFn<T, TableRow>
      implements RequiresWindowAccess {

    @Override
    public void processElement(ProcessContext c) {

      TableRow row = new TableRow();
      for (Map.Entry<String, FieldInfo<T>> entry : fieldInfo.entrySet()) {
          String key = entry.getKey();
          FieldInfo<T> fcnInfo = entry.getValue();
          SerializableFunction<OldDoFn<T, TableRow>.ProcessContext, Object> fcn =
            fcnInfo.getFieldFn();
          row.set(key, fcn.apply(c));
        }
      c.output(row);
    }
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

}
