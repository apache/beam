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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import java.util.concurrent.Callable;

/**
 * To fulfill multi-threaded execution
 */
public class TpcdsRun implements Callable<TpcdsRunResult> {
    private final Pipeline pipeline;

    public TpcdsRun (Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public TpcdsRunResult call() {
        TpcdsRunResult tpcdsRunResult;

        try {
            PipelineResult pipelineResult = pipeline.run();
            long startTimeStamp = System.currentTimeMillis();
            State state = pipelineResult.waitUntilFinish();
            long endTimeStamp = System.currentTimeMillis();

            // Make sure to set the job status to be successful only when pipelineResult's final state is DONE.
            boolean isSuccessful = state == State.DONE;
            tpcdsRunResult = new TpcdsRunResult(isSuccessful, startTimeStamp, endTimeStamp, pipeline.getOptions(), pipelineResult);
        } catch (Exception e) {
            // If the pipeline execution failed, return a result with failed status but don't interrupt other threads.
            e.printStackTrace();
            tpcdsRunResult = new TpcdsRunResult(false, 0, 0, pipeline.getOptions(), null);
        }

        return tpcdsRunResult;
    }
}
