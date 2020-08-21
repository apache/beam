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

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import java.sql.Timestamp;
import java.util.Date;


public class TpcdsRunResult {
    private boolean isSuccessful;
    private long startTime;
    private long endTime;
    private PipelineOptions pipelineOptions;
    private PipelineResult pipelineResult;

    public TpcdsRunResult(boolean isSuccessful, long startTime, long endTime, PipelineOptions pipelineOptions, PipelineResult pipelineResult) {
        this.isSuccessful = isSuccessful;
        this.startTime = startTime;
        this.endTime = endTime;
        this.pipelineOptions = pipelineOptions;
        this.pipelineResult = pipelineResult;
    }

    public boolean getIsSuccessful() { return isSuccessful; }

    public Date getStartDate() {
        Timestamp startTimeStamp = new Timestamp(startTime);
        Date startDate = new Date(startTimeStamp.getTime());
        return startDate;
    }

    public Date getEndDate() {
        Timestamp endTimeStamp = new Timestamp(endTime);
        Date endDate = new Date(endTimeStamp.getTime());
        return endDate;
    }

    public double getElapsedTime() {
        return (endTime - startTime) / 1000.0;
    }

    public PipelineOptions getPipelineOptions() { return pipelineOptions; }

    public PipelineResult getPipelineResult() { return pipelineResult; }

    public String getJobName() {
        PipelineOptions pipelineOptions = getPipelineOptions();
        return pipelineOptions.getJobName();
    }

    public String getQueryName() {
        String jobName = getJobName();
        int endIndex = jobName.indexOf("result");
        String queryName = jobName.substring(0, endIndex);
        return queryName;
    }

    public String getDataSize() throws Exception {
        PipelineOptions pipelineOptions = getPipelineOptions();
        return TpcdsParametersReader.getAndCheckDataSize(pipelineOptions.as(TpcdsOptions.class));
    }

    public String getDialect() throws Exception {
        PipelineOptions pipelineOptions = getPipelineOptions();
        String queryPlannerClassName = pipelineOptions.as(BeamSqlPipelineOptions.class).getPlannerName();
        String dialect;
        if (queryPlannerClassName.equals("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner")) {
            dialect = "ZetaSQL";
        } else {
            dialect = "Calcite";
        }
        return dialect;
    }
}
