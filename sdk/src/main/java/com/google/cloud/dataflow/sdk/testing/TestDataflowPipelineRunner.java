/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;

/**
 * TestDataflowPipelineRunner is a pipeline runner that wraps a
 * DataflowPipelineRunner when running tests against the {@link TestPipeline}.
 *
 * @see TestPipeline
 */
public class TestDataflowPipelineRunner extends BlockingDataflowPipelineRunner {
  TestDataflowPipelineRunner(
      DataflowPipelineRunner internalRunner,
      MonitoringUtil.JobMessagesHandler jobMessagesHandler) {
    super(internalRunner, jobMessagesHandler);
  }

  @Override
  public PipelineJobState run(Pipeline pipeline) {
    PipelineJobState state = super.run(pipeline);
    if (state.getJobState() != MonitoringUtil.JobState.DONE) {
      throw new AssertionError("The dataflow failed.");
    }
    return state;
  }
}
