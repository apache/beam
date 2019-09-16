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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A BigQueryTable that keeps jobName from the pipeline options whenever row count is called. It is
 * made for {@link BigQueryRowCountIT#testPipelineOptionInjection()}
 */
public class BigQueryTestTable extends BigQueryTable {
  private String jobName = null;

  BigQueryTestTable(Table table, BigQueryUtils.ConversionOptions options) {
    super(table, options);
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    jobName = options.getJobName();
    return super.getTableStatistics(options);
  }

  String getJobName() {
    return this.jobName;
  }
}
