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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.ExistingPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.DetectNewPartitionsState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.InitialPipelineState;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn responsible to initialize the metadata table and prepare it for managing the state of the
 * pipeline.
 */
@Internal
public class InitializeDoFn extends DoFn<byte[], InitialPipelineState> implements Serializable {
  private static final long serialVersionUID = 1868189906451252363L;
  private static final Logger LOG = LoggerFactory.getLogger(InitializeDoFn.class);
  private final DaoFactory daoFactory;
  private Instant startTime;
  private final ExistingPipelineOptions existingPipelineOptions;

  public InitializeDoFn(
      DaoFactory daoFactory, Instant startTime, ExistingPipelineOptions existingPipelineOptions) {
    this.daoFactory = daoFactory;
    this.startTime = startTime;
    this.existingPipelineOptions = existingPipelineOptions;
  }

  @ProcessElement
  public void processElement(OutputReceiver<InitialPipelineState> receiver) throws IOException {
    LOG.info(daoFactory.getStreamTableDebugString());
    LOG.info(daoFactory.getMetadataTableDebugString());
    LOG.info("ChangeStreamName: " + daoFactory.getChangeStreamName());

    boolean resume = false;
    DetectNewPartitionsState detectNewPartitionsState =
        daoFactory.getMetadataTableDao().readDetectNewPartitionsState();

    switch (existingPipelineOptions) {
      case RESUME_OR_NEW:
        // perform resumption.
        if (detectNewPartitionsState != null) {
          resume = true;
          startTime = detectNewPartitionsState.getWatermark();
          LOG.info("Resuming from previous pipeline with low watermark of {}", startTime);
        } else {
          LOG.info(
              "Attempted to resume, but previous watermark does not exist, starting at {}",
              startTime);
        }
        break;
      case RESUME_OR_FAIL:
        // perform resumption.
        if (detectNewPartitionsState != null) {
          resume = true;
          startTime = detectNewPartitionsState.getWatermark();
          LOG.info("Resuming from previous pipeline with low watermark of {}", startTime);
        } else {
          LOG.error("Previous pipeline with the same change stream name doesn't exist, stopping");
          return;
        }
        break;
      case FAIL_IF_EXISTS:
        if (detectNewPartitionsState != null) {
          LOG.error(
              "A previous pipeline exists with the same change stream name and existingPipelineOption is set to FAIL_IF_EXISTS.");
          return;
        }
        break;
      case SKIP_CLEANUP:
        if (detectNewPartitionsState != null) {
          LOG.error(
              "A previous pipeline exists with the same change stream name and existingPipelineOption is set to SKIP_CLEANUP. This option should only be used in tests.");
          return;
        }
        break;
      default:
        LOG.error("Unexpected existingPipelineOptions option.");
        // terminate pipeline
        return;
    }
    daoFactory.getMetadataTableDao().writeDetectNewPartitionVersion();
    receiver.output(new InitialPipelineState(startTime, resume));
  }
}
