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
package org.apache.beam.sdk.io.gcp.expansion.service;

import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.expansion.service.ExpansionServiceOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** An expansion service for GCP IOs. */
public class GcpExpansionService extends ExpansionService {
  public GcpExpansionService(String[] args) {
    super(args);
  }

  @Override
  protected PipelineOptions configPipelineOptions(PipelineOptions providedOpts) {
    PipelineOptions opts = super.configPipelineOptions(providedOpts);
    GcpOptions gcpOptions = opts.as(GcpOptions.class);
    GcpOptions specifiedOptions = providedOpts.as(GcpOptions.class);
    Optional.ofNullable(specifiedOptions.getProject()).ifPresent(gcpOptions::setProject);
    return opts;
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);

    // Register the options class used by the expansion service.
    PipelineOptionsFactory.register(ExpansionServiceOptions.class);

    @SuppressWarnings("nullness")
    ExpansionService service = new GcpExpansionService(Arrays.copyOfRange(args, 1, args.length));

    StartServer(service, port);
  }
}
