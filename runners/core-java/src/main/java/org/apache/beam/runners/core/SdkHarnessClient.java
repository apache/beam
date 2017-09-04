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
package org.apache.beam.runners.core;

import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.fn.v1.BeamFnApi;

/**
 * A high-level client for an SDK harness.
 *
 * <p>This provides a Java-friendly wrapper around {@link FnApiControlClient} and {@link
 * FnDataReceiver}, which handle lower-level gRPC message wrangling.
 */
public class SdkHarnessClient {

  /**
   * A supply of unique identifiers, used internally. These must be unique across all Fn API
   * clients.
   */
  public interface IdGenerator {
    String getId();
  }

  /** A supply of unique identifiers that are simply incrementing longs. */
  private static class CountingIdGenerator implements IdGenerator {
    private final AtomicLong nextId = new AtomicLong(0L);

    @Override
    public String getId() {
      return String.valueOf(nextId.incrementAndGet());
    }
  }

  /**
   * An active bundle for a particular {@link
   * org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor}.
   */
  @AutoValue
  public abstract static class ActiveBundle<InputT> {
    public abstract String getBundleId();

    public abstract Future<BeamFnApi.ProcessBundleResponse> getBundleResponse();

    public abstract FnDataReceiver<InputT> getInputReceiver();

    public static <InputT> ActiveBundle<InputT> create(
        String bundleId,
        Future<BeamFnApi.ProcessBundleResponse> response,
        FnDataReceiver<InputT> dataReceiver) {
      return new AutoValue_SdkHarnessClient_ActiveBundle(bundleId, response, dataReceiver);
    }
  }

  private final IdGenerator idGenerator;
  private final FnApiControlClient fnApiControlClient;

  private SdkHarnessClient(
      FnApiControlClient fnApiControlClient,
      IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
    this.fnApiControlClient = fnApiControlClient;
  }

  /**
   * Creates a client for a particular SDK harness. It is the responsibility of the caller to ensure
   * that these correspond to the same SDK harness, so control plane and data plane messages can be
   * correctly associated.
   */
  public static SdkHarnessClient usingFnApiClient(FnApiControlClient fnApiControlClient) {
    return new SdkHarnessClient(fnApiControlClient, new CountingIdGenerator());
  }

  public SdkHarnessClient withIdGenerator(IdGenerator idGenerator) {
    return new SdkHarnessClient(fnApiControlClient, idGenerator);
  }

  /**
   * Registers a {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor} for future
   * processing.
   *
   * <p>A client may block on the result future, but may also proceed without blocking.
   */
  public Future<BeamFnApi.RegisterResponse> register(
      Iterable<BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors) {

    // TODO: validate that all the necessary data endpoints are known

    ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setRegister(
                    BeamFnApi.RegisterRequest.newBuilder()
                        .addAllProcessBundleDescriptor(processBundleDescriptors)
                        .build())
                .build());

    return Futures.transform(
        genericResponse,
        new Function<BeamFnApi.InstructionResponse, BeamFnApi.RegisterResponse>() {
          @Override
          public BeamFnApi.RegisterResponse apply(BeamFnApi.InstructionResponse input) {
            return input.getRegister();
          }
        });
  }

  /**
   * Start a new bundle for the given {@link
   * org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor} identifier.
   *
   * <p>The input channels for the returned {@link ActiveBundle} are derived from the
   * instructions in the {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor}.
   */
  public ActiveBundle newBundle(String processBundleDescriptorId) {
    String bundleId = idGenerator.getId();

    // TODO: acquire an input receiver from appropriate FnDataService
    FnDataReceiver dataReceiver = new FnDataReceiver() {
      @Override
      public void accept(Object input) throws Exception {
        throw new UnsupportedOperationException("Placeholder FnDataReceiver cannot accept data.");
      }

      @Override
      public void close() throws IOException {
        // noop
      }
    };

    ListenableFuture<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setProcessBundle(
                    BeamFnApi.ProcessBundleRequest.newBuilder()
                        .setProcessBundleDescriptorReference(processBundleDescriptorId))
                .build());

    ListenableFuture<BeamFnApi.ProcessBundleResponse> specificResponse =
        Futures.transform(
            genericResponse,
            new Function<BeamFnApi.InstructionResponse, BeamFnApi.ProcessBundleResponse>() {
              @Override
              public BeamFnApi.ProcessBundleResponse apply(BeamFnApi.InstructionResponse input) {
                return input.getProcessBundle();
              }
            });

    return ActiveBundle.create(bundleId, specificResponse, dataReceiver);
  }
}
