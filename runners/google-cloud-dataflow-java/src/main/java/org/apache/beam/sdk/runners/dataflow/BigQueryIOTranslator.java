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
package org.apache.beam.sdk.runners.dataflow;

import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.runners.DataflowPipelineTranslator;
import org.apache.beam.sdk.util.PropertyNames;

import com.google.api.services.bigquery.model.TableReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQuery transform support code for the Dataflow backend.
 */
public class BigQueryIOTranslator {
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
}
