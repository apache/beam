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
package org.apache.beam.fn.harness.control;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler and datastore for types that be can be registered via the Fn API.
 *
 * <p>Allows for {@link BeamFnApi.RegisterRequest}s to occur in parallel with subsequent requests
 * that may lookup registered values by blocking lookups until registration occurs.
 */
public class RegisterHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterHandler.class);
  private final ConcurrentMap<String, CompletableFuture<Message>> idToObject;

  public RegisterHandler() {
    idToObject = new ConcurrentHashMap<>();
  }

  public Message getById(String id) {
    try {
      LOG.debug("Attempting to find {}", id);
      @SuppressWarnings("unchecked")
      CompletableFuture<Message> returnValue = computeIfAbsent(id);
      /*
       * TODO: Even though the register request instruction occurs before the process bundle
       * instruction in the control stream, the instructions are being processed in parallel
       * in the Java harness causing a data race which is why we use a future. This will block
       * forever in the case of a runner having a bug sending the wrong ids. Instead of blocking
       * forever, we could do a timed wait or come up with another way of ordering the instruction
       * processing to remove the data race.
       */
      return returnValue.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(String.format("Failed to load %s", id), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format("Failed to load %s", id), e);
    }
  }

  public BeamFnApi.InstructionResponse.Builder register(BeamFnApi.InstructionRequest request) {
    BeamFnApi.InstructionResponse.Builder response =
        BeamFnApi.InstructionResponse.newBuilder()
            .setRegister(RegisterResponse.getDefaultInstance());

    BeamFnApi.RegisterRequest registerRequest = request.getRegister();
    for (BeamFnApi.ProcessBundleDescriptor processBundleDescriptor :
        registerRequest.getProcessBundleDescriptorList()) {
      LOG.debug(
          "Registering {} with type {}",
          processBundleDescriptor.getId(),
          processBundleDescriptor.getClass());
      computeIfAbsent(processBundleDescriptor.getId()).complete(processBundleDescriptor);
      for (Map.Entry<String, RunnerApi.Coder> entry :
          processBundleDescriptor.getCodersMap().entrySet()) {
        LOG.debug("Registering {} with type {}", entry.getKey(), entry.getValue().getClass());
        computeIfAbsent(entry.getKey()).complete(entry.getValue());
      }
    }

    return response;
  }

  private CompletableFuture<Message> computeIfAbsent(String id) {
    return idToObject.computeIfAbsent(id, (String ignored) -> new CompletableFuture<>());
  }
}
