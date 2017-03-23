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
package org.apache.beam.integration.nexmark.drivers;

import org.apache.beam.integration.nexmark.NexmarkDriver;
import org.apache.beam.integration.nexmark.NexmarkOptions;
import org.apache.beam.integration.nexmark.model.Auction;
import org.apache.beam.integration.nexmark.model.Bid;
import org.apache.beam.integration.nexmark.model.Person;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * An implementation of the 'NEXMark queries' for Google Dataflow.
 * These are multiple queries over a three table schema representing an online auction system:
 * <ul>
 * <li>{@link Person} represents a person submitting an item for auction and/or making a bid
 * on an auction.
 * <li>{@link Auction} represents an item under auction.
 * <li>{@link Bid} represents a bid for an item under auction.
 * </ul>
 * The queries exercise many aspects of streaming dataflow.
 *
 * <p>We synthesize the creation of people, auctions and bids in real-time. The data is not
 * particularly sensible.
 *
 * <p>See <a href="http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * http://datalab.cs.pdx.edu/niagaraST/NEXMark/</a>
 */
class NexmarkGoogleDriver extends NexmarkDriver<NexmarkGoogleDriver.NexmarkGoogleOptions> {
  /**
   * Command line flags.
   */
  public interface NexmarkGoogleOptions extends NexmarkOptions, DataflowPipelineOptions {

  }

  /**
   * Entry point.
   */
  public static void main(String[] args) {
    // Gather command line args, baseline, configurations, etc.
    NexmarkGoogleOptions options = PipelineOptionsFactory.fromArgs(args)
                                                         .withValidation()
                                                         .as(NexmarkGoogleOptions.class);
    options.setRunner(DataflowRunner.class);
    NexmarkGoogleRunner runner = new NexmarkGoogleRunner(options);
    new NexmarkGoogleDriver().runAll(options, runner);
  }
}

