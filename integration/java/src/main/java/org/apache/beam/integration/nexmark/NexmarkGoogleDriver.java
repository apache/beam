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

package org.apache.beam.integration.nexmark;

import org.apache.beam.runners.dataflow.DataflowPipelineRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import javax.annotation.Nullable;

/**
 * An implementation of the 'NEXMark queries' for Google Dataflow.
 * These are 11 queries over a three table schema representing on online auction system:
 * <ul>
 * <li>{@link Person} represents a person submitting an item for auction and/or making a bid
 * on an auction.
 * <li>{@link Auction} represents an item under auction.
 * <li>{@link Bid} represents a bid for an item under auction.
 * </ul>
 * The queries exercise many aspects of streaming dataflow.
 * <p>
 * <p>We synthesize the creation of people, auctions and bids in real-time. The data is not
 * particularly sensible.
 * <p>
 * <p>See
 * <a href="http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * http://datalab.cs.pdx.edu/niagaraST/NEXMark/</a>
 */
class NexmarkGoogleDriver extends NexmarkDriver<NexmarkGoogleDriver.NexmarkGoogleOptions> {
  /**
   * Command line flags.
   */
  public interface NexmarkGoogleOptions extends Options, DataflowPipelineOptions {
    @Description("If set, cancel running pipelines after this long")
    @Nullable
    Long getRunningTimeMinutes();

    void setRunningTimeMinutes(Long value);

    @Description("If set and --monitorJobs is true, check that the system watermark is never more "
                 + "than this far behind real time")
    @Nullable
    Long getMaxSystemLagSeconds();

    void setMaxSystemLagSeconds(Long value);

    @Description("If set and --monitorJobs is true, check that the data watermark is never more "
                 + "than this far behind real time")
    @Nullable
    Long getMaxDataLagSeconds();

    void setMaxDataLagSeconds(Long value);

    @Description("Only start validating watermarks after this many seconds")
    @Nullable
    Long getWatermarkValidationDelaySeconds();

    void setWatermarkValidationDelaySeconds(Long value);
  }

  /**
   * Entry point.
   */
  public static void main(String[] args) {
    // Gather command line args, baseline, configurations, etc.
    NexmarkGoogleOptions options = PipelineOptionsFactory.fromArgs(args)
                                                         .withValidation()
                                                         .as(NexmarkGoogleOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
    NexmarkGoogleRunner runner = new NexmarkGoogleRunner(options);
    new NexmarkGoogleDriver().runAll(options, runner);
  }
}

