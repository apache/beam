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

package org.apache.beam.fn.harness;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.TextFormat;
import java.io.PrintStream;
import java.util.EnumMap;
import org.apache.beam.fn.harness.channel.ManagedChannelFactory;
import org.apache.beam.fn.harness.control.BeamFnControlClient;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.control.RegisterHandler;
import org.apache.beam.fn.harness.data.BeamFnDataGrpcClient;
import org.apache.beam.fn.harness.fn.ThrowingFunction;
import org.apache.beam.fn.harness.logging.BeamFnLoggingClient;
import org.apache.beam.fn.harness.stream.StreamObserverFactory;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point into the Beam SDK Fn Harness for Java.
 *
 * <p>This entry point expects the following environment variables:
 * <ul>
 *   <li>LOGGING_API_SERVICE_DESCRIPTOR: A
 *   {@link org.apache.beam.fn.v1.BeamFnApi.ApiServiceDescriptor} encoded as text
 *   representing the endpoint that is to be connected to for the Beam Fn Logging service.</li>
 *   <li>CONTROL_API_SERVICE_DESCRIPTOR: A
 *   {@link org.apache.beam.fn.v1.BeamFnApi.ApiServiceDescriptor} encoded as text
 *   representing the endpoint that is to be connected to for the Beam Fn Control service.</li>
 *   <li>PIPELINE_OPTIONS: A serialized form of {@link PipelineOptions}. See {@link PipelineOptions}
 *   for further details.</li>
 * </ul>
 */
public class FnHarness {
  private static final String CONTROL_API_SERVICE_DESCRIPTOR = "CONTROL_API_SERVICE_DESCRIPTOR";
  private static final String LOGGING_API_SERVICE_DESCRIPTOR = "LOGGING_API_SERVICE_DESCRIPTOR";
  private static final String PIPELINE_OPTIONS = "PIPELINE_OPTIONS";
  private static final Logger LOG = LoggerFactory.getLogger(FnHarness.class);

  private static BeamFnApi.ApiServiceDescriptor getApiServiceDescriptor(String env)
      throws TextFormat.ParseException {
    BeamFnApi.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        BeamFnApi.ApiServiceDescriptor.newBuilder();
    TextFormat.merge(System.getenv(env), apiServiceDescriptorBuilder);
    return apiServiceDescriptorBuilder.build();
  }

  public static void main(String[] args) throws Exception {
    System.out.format("SDK Fn Harness started%n");
    System.out.format("Logging location %s%n", System.getenv(LOGGING_API_SERVICE_DESCRIPTOR));
    System.out.format("Control location %s%n", System.getenv(CONTROL_API_SERVICE_DESCRIPTOR));
    System.out.format("Pipeline options %s%n", System.getenv(PIPELINE_OPTIONS));

    PipelineOptions options = new ObjectMapper().readValue(
        System.getenv(PIPELINE_OPTIONS), PipelineOptions.class);

    BeamFnApi.ApiServiceDescriptor loggingApiServiceDescriptor =
        getApiServiceDescriptor(LOGGING_API_SERVICE_DESCRIPTOR);

    BeamFnApi.ApiServiceDescriptor controlApiServiceDescriptor =
        getApiServiceDescriptor(CONTROL_API_SERVICE_DESCRIPTOR);

    main(options, loggingApiServiceDescriptor, controlApiServiceDescriptor);
  }

  public static void main(PipelineOptions options,
      BeamFnApi.ApiServiceDescriptor loggingApiServiceDescriptor,
      BeamFnApi.ApiServiceDescriptor controlApiServiceDescriptor) throws Exception {
    IOChannelUtils.registerIOFactories(options);

    ManagedChannelFactory channelFactory = ManagedChannelFactory.from(options);
    StreamObserverFactory streamObserverFactory = StreamObserverFactory.fromOptions(options);
    PrintStream originalErrStream = System.err;

    try (BeamFnLoggingClient logging = new BeamFnLoggingClient(
        options,
        loggingApiServiceDescriptor,
        channelFactory::forDescriptor,
        streamObserverFactory::from)) {

      LOG.info("Fn Harness started");
      EnumMap<BeamFnApi.InstructionRequest.RequestCase,
              ThrowingFunction<BeamFnApi.InstructionRequest,
                               BeamFnApi.InstructionResponse.Builder>> handlers =
          new EnumMap<>(BeamFnApi.InstructionRequest.RequestCase.class);

      RegisterHandler fnApiRegistry = new RegisterHandler();
      BeamFnDataGrpcClient beamFnDataMultiplexer = new BeamFnDataGrpcClient(
          options, channelFactory::forDescriptor, streamObserverFactory::from);

      ProcessBundleHandler processBundleHandler =
          new ProcessBundleHandler(options, fnApiRegistry::getById, beamFnDataMultiplexer);
      handlers.put(BeamFnApi.InstructionRequest.RequestCase.REGISTER,
          fnApiRegistry::register);
      handlers.put(BeamFnApi.InstructionRequest.RequestCase.PROCESS_BUNDLE,
          processBundleHandler::processBundle);
      BeamFnControlClient control = new BeamFnControlClient(controlApiServiceDescriptor,
          channelFactory::forDescriptor,
          streamObserverFactory::from,
          handlers);

      LOG.info("Entering instruction processing loop");
      control.processInstructionRequests(options.as(GcsOptions.class).getExecutorService());
    } catch (Throwable t) {
      t.printStackTrace(originalErrStream);
    } finally {
      originalErrStream.println("Shutting SDK harness down.");
    }
  }
}
