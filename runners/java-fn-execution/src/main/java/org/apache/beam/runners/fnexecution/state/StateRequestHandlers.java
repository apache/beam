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

package org.apache.beam.runners.fnexecution.state;

import static com.google.common.base.Preconditions.checkState;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.MultimapSideInputSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.Reiterable;

/**
 * A set of utility methods which construct {@link StateRequestHandler}s.
 *
 * <p>TODO: Add a variant which works on {@link ByteString}s to remove encoding/decoding overhead.
 */
public class StateRequestHandlers {

  /**
   * A handler for multimap side inputs.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface MultimapSideInputHandler<K, V, W extends BoundedWindow> {
    /**
     * Returns an {@link Iterable} of values representing the side input for the given key and
     * window.
     *
     * <p>TODO: Add support for side input chunking and caching if a {@link Reiterable} is returned.
     */
    Iterable<V> get(K key, W window);
  }

  /**
   * A factory which constructs {@link MultimapSideInputHandler}s.
   *
   * <p>Note that this factory should be thread safe because it will be invoked concurrently.
   */
  @ThreadSafe
  public interface MultimapSideInputHandlerFactory {

    /**
     * Returns a {@link MultimapSideInputHandler} for the given {@code pTransformId} and
     * {@code sideInputId}. The supplied {@code keyCoder}, {@code valueCoder}, and
     * {@code windowCoder} should be used to encode/decode their respective values.
     */
    <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
        String pTransformId,
        String sideInputId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /**
     * Throws a {@link UnsupportedOperationException} on the first access.
     */
    static MultimapSideInputHandlerFactory unsupported() {
      return new MultimapSideInputHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
            String pTransformId, String sideInputId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException(String.format(
              "The %s does not support handling sides inputs for PTransform %s with side "
                  + "input id %s.",
              MultimapSideInputHandler.class.getSimpleName(),
              pTransformId,
              sideInputId));
        }
      };
    }
  }

  /**
   * A handler for bag user state.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface BagUserStateHandler<K, V, W extends BoundedWindow> {
    /**
     * Returns an {@link Iterable} of values representing the bag user state for the given key and
     * window.
     *
     * <p>TODO: Add support for bag user state chunking and caching if a {@link Reiterable} is
     * returned.
     */
    Iterable<V> get(K key, W window);

    /**
     * Appends the values to the bag user state for the given key and window.
     */
    void append(K key, W window, Iterator<V> values);

    /**
     * Clears the bag user state for the given key and window.
     */
    void clear(K key, W window);
  }

  /**
   * A factory which constructs {@link BagUserStateHandler}s.
   *
   * <p>Note that this factory should be thread safe.
   */
  @ThreadSafe
  public interface BagUserStateHandlerFactory {
    <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
        String pTransformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /**
     * Throws a {@link UnsupportedOperationException} on the first access.
     */
    static BagUserStateHandlerFactory unsupported() {
      return new BagUserStateHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
            String pTransformId, String userStateId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException(String.format(
              "The %s does not support handling sides inputs for PTransform %s with user state "
                  + "id %s.",
              BagUserStateHandler.class.getSimpleName(),
              pTransformId,
              userStateId));
        }
      };
    }
  }

  /**
   * Returns an adapter which converts a {@link MultimapSideInputHandlerFactory} to a
   * {@link StateRequestHandler}.
   *
   * <p>The {@link MultimapSideInputHandlerFactory} is required to handle all multimap side inputs
   * contained within the {@link ExecutableProcessBundleDescriptor}. See
   * {@link ExecutableProcessBundleDescriptor#getMultimapSideInputSpecs} for the set of multimap
   * side inputs that are contained.
   *
   * <p>Instances of {@link MultimapSideInputHandler}s returned by the
   * {@link MultimapSideInputHandlerFactory} are cached.
   */
  public static StateRequestHandler forMultimapSideInputHandlerFactory(
      Map<String, Map<String, MultimapSideInputSpec>> sideInputSpecs,
      MultimapSideInputHandlerFactory multimapSideInputHandlerFactory) {
    return new StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter(
        sideInputSpecs, multimapSideInputHandlerFactory);
  }

  /**
   * An adapter which converts {@link MultimapSideInputHandlerFactory} to
   * {@link StateRequestHandler}.
   */
  static class StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter
      implements StateRequestHandler {

    private final Map<String, Map<String, MultimapSideInputSpec>> sideInputSpecs;
    private final MultimapSideInputHandlerFactory multimapSideInputHandlerFactory;
    private final ConcurrentHashMap<MultimapSideInputSpec, MultimapSideInputHandler> cache;

    StateRequestHandlerToMultimapSideInputHandlerFactoryAdapter(
        Map<String, Map<String, MultimapSideInputSpec>> sideInputSpecs,
        MultimapSideInputHandlerFactory multimapSideInputHandlerFactory) {
      this.sideInputSpecs = sideInputSpecs;
      this.multimapSideInputHandlerFactory = multimapSideInputHandlerFactory;
      this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(
        StateRequest request) throws Exception {
      try {
        checkState(TypeCase.MULTIMAP_SIDE_INPUT.equals(request.getStateKey().getTypeCase()),
            "Unsupported %s type %s, expected %s",
            StateRequest.class.getSimpleName(),
            request.getStateKey().getTypeCase(),
            TypeCase.MULTIMAP_SIDE_INPUT);

        StateKey.MultimapSideInput stateKey = request.getStateKey().getMultimapSideInput();
        MultimapSideInputSpec<?, ?, ?> sideInputReferenceSpec =
            sideInputSpecs.get(stateKey.getPtransformId()).get(stateKey.getSideInputId());
        MultimapSideInputHandler<?, ?, ?> handler = cache.computeIfAbsent(
            sideInputReferenceSpec,
            this::createHandler);

        switch (request.getRequestCase()) {
          case GET:
            return handleGetRequest(request, handler);
          case APPEND:
          case CLEAR:
          default:
            throw new Exception(String.format(
                "Unsupported request type %s for side input.", request.getRequestCase()));
        }
      } catch (Exception e) {
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(e);
        return f;
      }
    }

    private <K, V, W extends BoundedWindow> CompletionStage<StateResponse.Builder> handleGetRequest(
        StateRequest request, MultimapSideInputHandler<K, V, W> handler) throws Exception {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      StateKey.MultimapSideInput stateKey = request.getStateKey().getMultimapSideInput();

      MultimapSideInputSpec<K, V, W> sideInputReferenceSpec =
          sideInputSpecs
              .get(stateKey.getPtransformId())
              .get(stateKey.getSideInputId());

      K key = sideInputReferenceSpec.keyCoder().decode(stateKey.getKey().newInput());
      W window = sideInputReferenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

      Iterable<V> values = handler.get(key, window);
      List<ByteString> encodedValues = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = DataStreams.outbound(encodedValues::add);
      for (V value : values) {
        sideInputReferenceSpec.valueCoder().encode(value, outputStream);
        outputStream.delimitElement();
      }
      outputStream.close();

      StateResponse.Builder response = StateResponse.newBuilder();
      response.setId(request.getId());
      response.setGet(
          StateGetResponse.newBuilder()
              .setData(ByteString.copyFrom(encodedValues))
              .build());
      return CompletableFuture.completedFuture(response);
    }

    private <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> createHandler(
        MultimapSideInputSpec cacheKey) {
      return multimapSideInputHandlerFactory.forSideInput(
          cacheKey.transformId(),
          cacheKey.sideInputId(),
          cacheKey.keyCoder(),
          cacheKey.valueCoder(),
          cacheKey.windowCoder());
    }
  }
}
