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

import com.google.protobuf.Message;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.fn.v1.BeamFnApi.RegisterResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler and datastore for types that be can be registered via the Fn API.
 *
 * <p>Allows for {@link org.apache.beam.fn.v1.BeamFnApi.RegisterRequest}s to occur in parallel with
 * subsequent requests that may lookup registered values by blocking lookups until registration
 * occurs.
 */
public class RegisterHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterHandler.class);
  private final ConcurrentMap<Long, CompletableFuture<Message>> idToObject;

  public RegisterHandler() {
    idToObject = new ConcurrentHashMap<>();
  }

  public <T extends Message> T getById(long id) {
    try {
      @SuppressWarnings("unchecked")
      CompletableFuture<T> returnValue = (CompletableFuture<T>) computeIfAbsent(id);
      return returnValue.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(String.format("Failed to load %s", id), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format("Failed to load %s", id), e);
    }
  }

  public BeamFnApi.InstructionResponse.Builder register(BeamFnApi.InstructionRequest request) {
    BeamFnApi.InstructionResponse.Builder response = BeamFnApi.InstructionResponse.newBuilder()
        .setRegister(RegisterResponse.getDefaultInstance());

    BeamFnApi.RegisterRequest registerRequest = request.getRegister();
    for (BeamFnApi.ProcessBundleDescriptor processBundleDescriptor
        : registerRequest.getProcessBundleDescriptorList()) {
      LOGGER.debug("Registering {} with type {}",
          processBundleDescriptor.getId(),
          processBundleDescriptor.getClass());
      computeIfAbsent(processBundleDescriptor.getId()).complete(processBundleDescriptor);
      for (BeamFnApi.Coder coder : processBundleDescriptor.getCodersList()) {
        LOGGER.debug("Registering {} with type {}",
            coder.getFunctionSpec().getId(),
            coder.getClass());
        computeIfAbsent(coder.getFunctionSpec().getId()).complete(coder);
      }
    }

    return response;
  }

  private CompletableFuture<Message> computeIfAbsent(long id) {
    return idToObject.computeIfAbsent(id, (Long ignored) -> new CompletableFuture<>());
  }
}
