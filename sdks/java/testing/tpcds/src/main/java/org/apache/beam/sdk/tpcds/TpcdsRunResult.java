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
package org.apache.beam.sdk.tpcds;

import java.sql.Timestamp;
import java.util.Date;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TpcdsRunResult {
  private final boolean isSuccessful;
  private final long startTime;
  private final long endTime;
  private final PipelineOptions pipelineOptions;
  private final @Nullable PipelineResult pipelineResult;

  public TpcdsRunResult(
      boolean isSuccessful,
      long startTime,
      long endTime,
      PipelineOptions pipelineOptions,
      @Nullable PipelineResult pipelineResult) {
    this.isSuccessful = isSuccessful;
    this.startTime = startTime;
    this.endTime = endTime;
    this.pipelineOptions = pipelineOptions;
    this.pipelineResult = pipelineResult;
  }

  public boolean getIsSuccessful() {
    return isSuccessful;
  }

  public Date getStartDate() {
    Timestamp startTimeStamp = new Timestamp(startTime);
    return new Date(startTimeStamp.getTime());
  }

  public Date getEndDate() {
    Timestamp endTimeStamp = new Timestamp(endTime);
    return new Date(endTimeStamp.getTime());
  }

  public double getElapsedTime() {
    return (endTime - startTime) / 1000.0;
  }

  public PipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public @Nullable PipelineResult getPipelineResult() {
    return pipelineResult;
  }

  public String getJobName() {
    PipelineOptions pipelineOptions = getPipelineOptions();
    return pipelineOptions.getJobName();
  }

  public String getQueryName() {
    String jobName = getJobName();
    int endIndex = jobName.indexOf("result");
    return jobName.substring(0, endIndex);
  }

  public String getDataSize() {
    PipelineOptions pipelineOptions = getPipelineOptions();
    return pipelineOptions.as(TpcdsOptions.class).getDataSize();
  }

  public String getDialect() {
    PipelineOptions pipelineOptions = getPipelineOptions();
    String queryPlannerClassName =
        pipelineOptions.as(BeamSqlPipelineOptions.class).getPlannerName();
    String dialect;
    if (queryPlannerClassName.equals(
        "org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner")) {
      dialect = "ZetaSQL";
    } else {
      dialect = "Calcite";
    }
    return dialect;
  }
}
