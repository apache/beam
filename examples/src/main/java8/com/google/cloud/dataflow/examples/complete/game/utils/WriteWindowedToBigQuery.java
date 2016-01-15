  /*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.complete.game.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

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
  protected class BuildRowFn extends DoFn<T, TableRow>
      implements RequiresWindowAccess {

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

  @Override
  public PDone apply(PCollection<T> teamAndScore) {
    return teamAndScore
      .apply(ParDo.named("ConvertToRow").of(new BuildRowFn()))
      .apply(BigQueryIO.Write
                .to(getTable(teamAndScore.getPipeline(),
                    tableName))
                .withSchema(getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
  }

}
