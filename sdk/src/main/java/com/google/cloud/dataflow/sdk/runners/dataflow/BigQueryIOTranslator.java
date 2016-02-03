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

package com.google.cloud.dataflow.sdk.runners.dataflow;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * BigQuery transform support code for the Dataflow backend.
 */
public class BigQueryIOTranslator {
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOTranslator.class);

  /**
   * Implements BigQueryIO Read translation for the Dataflow backend.
   */
  public static class ReadTranslator
      implements DataflowPipelineTranslator.TransformTranslator<BigQueryIO.Read.Bound> {

    @Override
    public void translate(
        BigQueryIO.Read.Bound transform, DataflowPipelineTranslator.TranslationContext context) {
      // Actual translation.
      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, "bigquery");
      context.addInput(PropertyNames.BIGQUERY_EXPORT_FORMAT, "FORMAT_AVRO");

      if (transform.getQuery() != null) {
        context.addInput(PropertyNames.BIGQUERY_QUERY, transform.getQuery());
        context.addInput(PropertyNames.BIGQUERY_FLATTEN_RESULTS, transform.getFlattenResults());
      } else {
        TableReference table = transform.getTable();
        if (table.getProjectId() == null) {
          // If user does not specify a project we assume the table to be located in the project
          // that owns the Dataflow job.
          String projectIdFromOptions = context.getPipelineOptions().getProject();
          LOG.warn(String.format(BigQueryIO.SET_PROJECT_FROM_OPTIONS_WARNING, table.getDatasetId(),
              table.getDatasetId(), table.getTableId(), projectIdFromOptions));
          table.setProjectId(projectIdFromOptions);
        }

        context.addInput(PropertyNames.BIGQUERY_TABLE, table.getTableId());
        context.addInput(PropertyNames.BIGQUERY_DATASET, table.getDatasetId());
        if (table.getProjectId() != null) {
          context.addInput(PropertyNames.BIGQUERY_PROJECT, table.getProjectId());
        }
      }
      context.addValueOnlyOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
  }

  /**
   * Implements BigQueryIO Write translation for the Dataflow backend.
   */
  public static class WriteTranslator
      implements DataflowPipelineTranslator.TransformTranslator<BigQueryIO.Write.Bound> {

    @Override
    public void translate(BigQueryIO.Write.Bound transform,
                          DataflowPipelineTranslator.TranslationContext context) {
      if (context.getPipelineOptions().isStreaming()) {
        // Streaming is handled by the streaming runner.
        throw new AssertionError(
            "BigQueryIO is specified to use streaming write in batch mode.");
      }

      TableReference table = transform.getTable();

      // Actual translation.
      context.addStep(transform, "ParallelWrite");
      context.addInput(PropertyNames.FORMAT, "bigquery");
      context.addInput(PropertyNames.BIGQUERY_TABLE,
                       table.getTableId());
      context.addInput(PropertyNames.BIGQUERY_DATASET,
                       table.getDatasetId());
      if (table.getProjectId() != null) {
        context.addInput(PropertyNames.BIGQUERY_PROJECT, table.getProjectId());
      }
      if (transform.getSchema() != null) {
        try {
          context.addInput(PropertyNames.BIGQUERY_SCHEMA,
                           JSON_FACTORY.toString(transform.getSchema()));
        } catch (IOException exn) {
          throw new IllegalArgumentException("Invalid table schema.", exn);
        }
      }
      context.addInput(
          PropertyNames.BIGQUERY_CREATE_DISPOSITION,
          transform.getCreateDisposition().name());
      context.addInput(
          PropertyNames.BIGQUERY_WRITE_DISPOSITION,
          transform.getWriteDisposition().name());
      // Set sink encoding to TableRowJsonCoder.
      context.addEncodingInput(
          WindowedValue.getValueOnlyCoder(TableRowJsonCoder.of()));
      context.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));
    }
  }
}
