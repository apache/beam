/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceOperationRequestToSourceOperationRequest;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceOperationResponseToCloudSourceOperationResponse;

import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An executor for a source operation, defined by a {@code SourceOperationRequest}.
 */
@SuppressWarnings("resource")
public class SourceOperationExecutor extends WorkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MapTaskExecutor.class);

  private final PipelineOptions options;
  private final SourceOperationRequest request;
  private SourceOperationResponse response;

  public SourceOperationExecutor(PipelineOptions options,
                                 SourceOperationRequest request,
                                 CounterSet counters) {
    super(counters);
    this.options = options;
    this.request = request;
  }

  @Override
  public void execute() throws Exception {
    LOG.debug("Executing source operation");

    Source sourceSpec;
    if (request.getGetMetadata() != null) {
      sourceSpec = request.getGetMetadata().getSource();
    } else if (request.getSplit() != null) {
      sourceSpec = request.getSplit().getSource();
    } else {
      throw new UnsupportedOperationException("Unknown source operation");
    }

    this.response = sourceOperationResponseToCloudSourceOperationResponse(
        SourceFormatFactory.create(options, sourceSpec)
            .performSourceOperation(
                cloudSourceOperationRequestToSourceOperationRequest(request)));

    LOG.debug("Source operation execution complete");
  }

  public SourceOperationResponse getResponse() {
    return response;
  }
}
