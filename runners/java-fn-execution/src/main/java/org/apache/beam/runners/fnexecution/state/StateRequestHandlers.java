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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.BagUserStateSpec;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.SideInputSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.fn.stream.DataStreams.ElementDelimitedOutputStream;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.sdk.v2.sdk.extensions.protobuf.ByteStringCoder;

/**
 * A set of utility methods which construct {@link StateRequestHandler}s.
 *
 * <p>TODO: Add a variant which works on {@link ByteString}s to remove encoding/decoding overhead.
 */
public class StateRequestHandlers {

  /**
   * Marker interface that denotes some type of side input handler. The access pattern defines the
   * underlying type.
   *
   * <ul>
   *   <li>access pattern: Java type
   *   <li>{@code beam:side_input:iterable:v1}: {@link IterableSideInputHandler}
   *   <li>{@code beam:side_input:multimap:v1}: {@link MultimapSideInputHandler}
   * </ul>
   */
  @ThreadSafe
  public interface SideInputHandler {}

  /**
   * A handler for iterable side inputs.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface IterableSideInputHandler<V, W extends BoundedWindow> extends SideInputHandler {
    /**
     * Returns an {@link Iterable} of values representing the side input for the given window.
     *
     * <p>TODO: Add support for side input chunking and caching if a {@link Reiterable} is returned.
     */
    Iterable<V> get(W window);

    /** Returns the {@link Coder} to use for the elements of the resulting values iterable. */
    Coder<V> elementCoder();
  }

  /**
   * A handler for multimap side inputs.
   *
   * <p>Note that this handler is expected to be thread safe as it will be invoked concurrently.
   */
  @ThreadSafe
  public interface MultimapSideInputHandler<K, V, W extends BoundedWindow>
      extends SideInputHandler {
    /**
     * Returns an {@link Iterable} of keys representing the side input for the given window.
     *
     * <p>TODO: Add support for side input chunking and caching if a {@link Reiterable} is returned.
     */
    Iterable<K> get(W window);

    /**
     * Returns an {@link Iterable} of values representing the side input for the given key and
     * window.
     *
     * <p>TODO: Add support for side input chunking and caching if a {@link Reiterable} is returned.
     */
    Iterable<V> get(K key, W window);

    /** Returns the {@link Coder} to use for the elements of the resulting keys iterable. */
    Coder<K> keyCoder();

    /** Returns the {@link Coder} to use for the elements of the resulting values iterable. */
    Coder<V> valueCoder();
  }

  /**
   * A factory which constructs {@link MultimapSideInputHandler}s.
   *
   * <p>Note that this factory should be thread safe because it will be invoked concurrently.
   */
  @ThreadSafe
  public interface SideInputHandlerFactory {

    /**
     * Returns an {@link IterableSideInputHandler} for the given {@code pTransformId}, {@code
     * sideInputId}. The supplied {@code elementCoder} and {@code windowCoder} should be used to
     * encode/decode their respective values.
     */
    <V, W extends BoundedWindow> IterableSideInputHandler<V, W> forIterableSideInput(
        String pTransformId, String sideInputId, Coder<V> elementCoder, Coder<W> windowCoder);

    /**
     * Returns a {@link MultimapSideInputHandler} for the given {@code pTransformId}, {@code
     * sideInputId}. The supplied {@code elementCoder} and {@code windowCoder} should be used to
     * encode/decode their respective values.
     */
    <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forMultimapSideInput(
        String pTransformId, String sideInputId, KvCoder<K, V> elementCoder, Coder<W> windowCoder);

    /** Throws a {@link UnsupportedOperationException} on the first access. */
    static SideInputHandlerFactory unsupported() {
      return new SideInputHandlerFactory() {
        @Override
        public <V, W extends BoundedWindow> IterableSideInputHandler<V, W> forIterableSideInput(
            String pTransformId, String sideInputId, Coder<V> elementCoder, Coder<W> windowCoder) {
          throw new UnsupportedOperationException(
              String.format(
                  "The %s does not support handling sides inputs for PTransform %s with side "
                      + "input id %s.",
                  SideInputHandlerFactory.class.getSimpleName(), pTransformId, sideInputId));
        }

        @Override
        public <K, V, W extends BoundedWindow>
            MultimapSideInputHandler<K, V, W> forMultimapSideInput(
                String pTransformId,
                String sideInputId,
                KvCoder<K, V> elementCoder,
                Coder<W> windowCoder) {
          throw new UnsupportedOperationException(
              String.format(
                  "The %s does not support handling sides inputs for PTransform %s with side "
                      + "input id %s.",
                  SideInputHandlerFactory.class.getSimpleName(), pTransformId, sideInputId));
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

    /** Appends the values to the bag user state for the given key and window. */
    void append(K key, W window, Iterator<V> values);

    /** Clears the bag user state for the given key and window. */
    void clear(K key, W window);
  }

  /**
   * A factory which constructs {@link BagUserStateHandler}s.
   *
   * <p>Note that this factory should be thread safe.
   */
  @ThreadSafe
  public interface BagUserStateHandlerFactory<K, V, W extends BoundedWindow> {
    BagUserStateHandler<K, V, W> forUserState(
        String pTransformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /** Throws a {@link UnsupportedOperationException} on the first access. */
    static <K, V, W extends BoundedWindow> BagUserStateHandlerFactory<K, V, W> unsupported() {
      return (pTransformId, userStateId, keyCoder, valueCoder, windowCoder) -> {
        throw new UnsupportedOperationException(
            String.format(
                "The %s does not support handling sides inputs for PTransform %s with user state "
                    + "id %s.",
                BagUserStateHandler.class.getSimpleName(), pTransformId, userStateId));
      };
    }
  }

  /**
   * Returns a {@link StateRequestHandler} which delegates to the supplied handler depending on the
   * {@link StateRequest}s {@link StateKey.TypeCase type}.
   *
   * <p>An exception is thrown if a corresponding handler is not found.
   */
  public static StateRequestHandler delegateBasedUponType(
      EnumMap<StateKey.TypeCase, StateRequestHandler> handlers) {
    return new StateKeyTypeDelegatingStateRequestHandler(handlers);
  }

  /**
   * A {@link StateRequestHandler} which delegates to the supplied handler depending on the {@link
   * StateRequest}s {@link StateKey.TypeCase type}.
   *
   * <p>An exception is thrown if a corresponding handler is not found.
   */
  static class StateKeyTypeDelegatingStateRequestHandler implements StateRequestHandler {
    private final EnumMap<TypeCase, StateRequestHandler> handlers;

    StateKeyTypeDelegatingStateRequestHandler(
        EnumMap<StateKey.TypeCase, StateRequestHandler> handlers) {
      this.handlers = handlers;
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(StateRequest request) throws Exception {
      return handlers
          .getOrDefault(request.getStateKey().getTypeCase(), this::handlerNotFound)
          .handle(request);
    }

    @Override
    public Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> getCacheTokens() {
      // Use loops here due to the horrible performance of Java Streams:
      // https://medium.com/@milan.mimica/slow-like-a-stream-fast-like-a-loop-524f70391182
      Set<BeamFnApi.ProcessBundleRequest.CacheToken> cacheTokens = new HashSet<>();
      for (StateRequestHandler handler : handlers.values()) {
        for (BeamFnApi.ProcessBundleRequest.CacheToken cacheToken : handler.getCacheTokens()) {
          cacheTokens.add(cacheToken);
        }
      }
      return cacheTokens;
    }

    private CompletionStage<StateResponse.Builder> handlerNotFound(StateRequest request) {
      CompletableFuture<StateResponse.Builder> rval = new CompletableFuture<>();
      rval.completeExceptionally(new IllegalStateException());
      return rval;
    }
  }

  /**
   * Returns an adapter which converts a {@link SideInputHandlerFactory} to a {@link
   * StateRequestHandler}.
   *
   * <p>The {@link SideInputHandlerFactory} is required to handle all side inputs contained within
   * the {@link ExecutableProcessBundleDescriptor}. See {@link
   * ExecutableProcessBundleDescriptor#getSideInputSpecs} for the set of side inputs that are
   * contained.
   *
   * <p>Instances of {@link MultimapSideInputHandler}s returned by the {@link
   * SideInputHandlerFactory} are cached.
   */
  public static StateRequestHandler forSideInputHandlerFactory(
      Map<String, Map<String, SideInputSpec>> sideInputSpecs,
      SideInputHandlerFactory sideInputHandlerFactory) {
    return new StateRequestHandlerToSideInputHandlerFactoryAdapter(
        sideInputSpecs, sideInputHandlerFactory);
  }

  /** An adapter which converts {@link SideInputHandlerFactory} to {@link StateRequestHandler}. */
  static class StateRequestHandlerToSideInputHandlerFactoryAdapter implements StateRequestHandler {

    private final Map<String, Map<String, SideInputSpec>> sideInputSpecs;
    private final SideInputHandlerFactory sideInputHandlerFactory;
    private final ConcurrentHashMap<SideInputSpec, SideInputHandler> handlerCache;

    StateRequestHandlerToSideInputHandlerFactoryAdapter(
        Map<String, Map<String, SideInputSpec>> sideInputSpecs,
        SideInputHandlerFactory sideInputHandlerFactory) {
      this.sideInputSpecs = sideInputSpecs;
      this.sideInputHandlerFactory = sideInputHandlerFactory;
      this.handlerCache = new ConcurrentHashMap<>();
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(StateRequest request) throws Exception {
      checkState(
          RequestCase.GET.equals(request.getRequestCase()),
          String.format("Unsupported request type %s for side input.", request.getRequestCase()));

      try {
        switch (request.getStateKey().getTypeCase()) {
          case MULTIMAP_SIDE_INPUT:
            {
              StateKey.MultimapSideInput stateKey = request.getStateKey().getMultimapSideInput();
              SideInputSpec<?, ?> referenceSpec =
                  sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());
              MultimapSideInputHandler handler =
                  (MultimapSideInputHandler)
                      handlerCache.computeIfAbsent(referenceSpec, this::createHandler);
              return handleGetMultimapValuesRequest(request, handler);
            }
          case MULTIMAP_KEYS_SIDE_INPUT:
            {
              StateKey.MultimapKeysSideInput stateKey =
                  request.getStateKey().getMultimapKeysSideInput();
              SideInputSpec<?, ?> referenceSpec =
                  sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());
              MultimapSideInputHandler handler =
                  (MultimapSideInputHandler)
                      handlerCache.computeIfAbsent(referenceSpec, this::createHandler);
              return handleGetMultimapKeysRequest(request, handler);
            }
          case ITERABLE_SIDE_INPUT:
            {
              StateKey.IterableSideInput stateKey = request.getStateKey().getIterableSideInput();
              SideInputSpec<?, ?> referenceSpec =
                  sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());
              IterableSideInputHandler handler =
                  (IterableSideInputHandler)
                      handlerCache.computeIfAbsent(referenceSpec, this::createHandler);
              return handleGetIterableValuesRequest(request, handler);
            }
          default:
            throw new IllegalStateException(
                String.format(
                    "Unsupported %s type %s, expected %s or %s",
                    StateRequest.class.getSimpleName(),
                    request.getStateKey().getTypeCase(),
                    TypeCase.MULTIMAP_SIDE_INPUT,
                    TypeCase.MULTIMAP_KEYS_SIDE_INPUT));
        }
      } catch (Exception e) {
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(e);
        return f;
      }
    }

    private <K, V, W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleGetMultimapKeysRequest(
            StateRequest request, MultimapSideInputHandler<K, V, W> handler) throws Exception {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(
          request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      StateKey.MultimapKeysSideInput stateKey = request.getStateKey().getMultimapKeysSideInput();

      SideInputSpec<KV<K, V>, W> sideInputReferenceSpec =
          sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());

      W window = sideInputReferenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

      Iterable<K> keys = handler.get(window);
      List<ByteString> encodedValues = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = DataStreams.outbound(encodedValues::add);
      for (K key : keys) {
        handler.keyCoder().encode(key, outputStream);
        outputStream.delimitElement();
      }
      outputStream.close();

      StateResponse.Builder response = StateResponse.newBuilder();
      response.setId(request.getId());
      response.setGet(
          StateGetResponse.newBuilder().setData(ByteString.copyFrom(encodedValues)).build());
      return CompletableFuture.completedFuture(response);
    }

    private <K, V, W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleGetMultimapValuesRequest(
            StateRequest request, MultimapSideInputHandler<K, V, W> handler) throws Exception {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(
          request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      StateKey.MultimapSideInput stateKey = request.getStateKey().getMultimapSideInput();

      SideInputSpec<KV<K, V>, W> sideInputReferenceSpec =
          sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());

      W window = sideInputReferenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

      Iterable<V> values =
          handler.get(handler.keyCoder().decode(stateKey.getKey().newInput()), window);
      List<ByteString> encodedValues = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = DataStreams.outbound(encodedValues::add);
      for (V value : values) {
        handler.valueCoder().encode(value, outputStream);
        outputStream.delimitElement();
      }
      outputStream.close();

      StateResponse.Builder response = StateResponse.newBuilder();
      response.setId(request.getId());
      response.setGet(
          StateGetResponse.newBuilder().setData(ByteString.copyFrom(encodedValues)).build());
      return CompletableFuture.completedFuture(response);
    }

    private <V, W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleGetIterableValuesRequest(
            StateRequest request, IterableSideInputHandler<V, W> handler) throws Exception {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(
          request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      StateKey.IterableSideInput stateKey = request.getStateKey().getIterableSideInput();

      SideInputSpec<V, W> sideInputReferenceSpec =
          sideInputSpecs.get(stateKey.getTransformId()).get(stateKey.getSideInputId());

      W window = sideInputReferenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

      Iterable<V> values = handler.get(window);
      List<ByteString> encodedValues = new ArrayList<>();
      ElementDelimitedOutputStream outputStream = DataStreams.outbound(encodedValues::add);
      for (V value : values) {
        handler.elementCoder().encode(value, outputStream);
        outputStream.delimitElement();
      }
      outputStream.close();

      StateResponse.Builder response = StateResponse.newBuilder();
      response.setId(request.getId());
      response.setGet(
          StateGetResponse.newBuilder().setData(ByteString.copyFrom(encodedValues)).build());
      return CompletableFuture.completedFuture(response);
    }

    private SideInputHandler createHandler(SideInputSpec<?, ?> cacheKey) {
      switch (cacheKey.accessPattern().getUrn()) {
        case Materializations.ITERABLE_MATERIALIZATION_URN:
          return sideInputHandlerFactory.forIterableSideInput(
              cacheKey.transformId(),
              cacheKey.sideInputId(),
              cacheKey.elementCoder(),
              cacheKey.windowCoder());

        case Materializations.MULTIMAP_MATERIALIZATION_URN:
          return sideInputHandlerFactory.forMultimapSideInput(
              cacheKey.transformId(),
              cacheKey.sideInputId(),
              (KvCoder) cacheKey.elementCoder(),
              cacheKey.windowCoder());

        default:
          throw new IllegalStateException(
              String.format("Unsupported access pattern for side input %s", cacheKey));
      }
    }
  }

  /**
   * Returns an adapter which converts a {@link BagUserStateHandlerFactory} to a {@link
   * StateRequestHandler}.
   *
   * <p>The {@link SideInputHandlerFactory} is required to handle all multimap side inputs contained
   * within the {@link ExecutableProcessBundleDescriptor}. See {@link
   * ExecutableProcessBundleDescriptor#getSideInputSpecs} for the set of multimap side inputs that
   * are contained.
   *
   * <p>Instances of {@link MultimapSideInputHandler}s returned by the {@link
   * SideInputHandlerFactory} are cached.
   *
   * <p>In case of any failures, this handler must be discarded. Otherwise, the contained state
   * cache token would be reused which would corrupt the state cache.
   */
  public static StateRequestHandler forBagUserStateHandlerFactory(
      ExecutableProcessBundleDescriptor processBundleDescriptor,
      BagUserStateHandlerFactory bagUserStateHandlerFactory) {
    return new ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter(
        processBundleDescriptor, bagUserStateHandlerFactory);
  }

  /**
   * An adapter which converts {@link BagUserStateHandlerFactory} to {@link StateRequestHandler}.
   */
  static class ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter
      implements StateRequestHandler {

    private final ExecutableProcessBundleDescriptor processBundleDescriptor;
    private final BagUserStateHandlerFactory handlerFactory;
    private final ConcurrentHashMap<BagUserStateSpec, BagUserStateHandler> handlerCache;
    private final BeamFnApi.ProcessBundleRequest.CacheToken cacheToken;

    ByteStringStateRequestHandlerToBagUserStateHandlerFactoryAdapter(
        ExecutableProcessBundleDescriptor processBundleDescriptor,
        BagUserStateHandlerFactory handlerFactory) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.handlerFactory = handlerFactory;
      this.handlerCache = new ConcurrentHashMap<>();
      this.cacheToken = createCacheToken();
    }

    @Override
    public CompletionStage<StateResponse.Builder> handle(StateRequest request) throws Exception {
      try {
        checkState(
            TypeCase.BAG_USER_STATE.equals(request.getStateKey().getTypeCase()),
            "Unsupported %s type %s, expected %s",
            StateRequest.class.getSimpleName(),
            request.getStateKey().getTypeCase(),
            TypeCase.BAG_USER_STATE);

        StateKey.BagUserState stateKey = request.getStateKey().getBagUserState();
        BagUserStateSpec<Object, Object, BoundedWindow> referenceSpec =
            processBundleDescriptor
                .getBagUserStateSpecs()
                .get(stateKey.getTransformId())
                .get(stateKey.getUserStateId());

        // Note that by using the ByteStringCoder, we simplify the issue of encoding/decoding the
        // logical stream because we do not need to maintain knowledge of element boundaries and
        // instead we rely on the client to be internally consistent. This allows us to just
        // take the append requests and also to serve them back without internal knowledge.
        checkState(
            ((Coder) referenceSpec.keyCoder()) instanceof ByteStringCoder,
            "This %s only supports the %s as the key coder.",
            BagUserStateHandlerFactory.class.getSimpleName(),
            ByteStringCoder.class.getSimpleName());
        checkState(
            ((Coder) referenceSpec.valueCoder()) instanceof ByteStringCoder,
            "This %s only supports the %s as the value coder.",
            BagUserStateHandlerFactory.class.getSimpleName(),
            ByteStringCoder.class.getSimpleName());

        BagUserStateHandler<ByteString, ByteString, BoundedWindow> handler =
            handlerCache.computeIfAbsent(referenceSpec, this::createHandler);

        ByteString key = stateKey.getKey();
        BoundedWindow window = referenceSpec.windowCoder().decode(stateKey.getWindow().newInput());

        switch (request.getRequestCase()) {
          case GET:
            return handleGetRequest(request, key, window, handler);
          case APPEND:
            return handleAppendRequest(request, key, window, handler);
          case CLEAR:
            return handleClearRequest(request, key, window, handler);
          default:
            throw new Exception(
                String.format(
                    "Unsupported request type %s for user state.", request.getRequestCase()));
        }
      } catch (Exception e) {
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(e);
        return f;
      }
    }

    @Override
    public Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> getCacheTokens() {
      return Collections.singleton(cacheToken);
    }

    private static <W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleGetRequest(
            StateRequest request,
            ByteString key,
            W window,
            BagUserStateHandler<ByteString, ByteString, W> handler) {
      // TODO: Add support for continuation tokens when handling state if the handler
      // returned a {@link Reiterable}.
      checkState(
          request.getGet().getContinuationToken().isEmpty(),
          "Continuation tokens are unsupported.");

      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(request.getId())
              .setGet(
                  StateGetResponse.newBuilder()
                      // Note that this doesn't copy the actual bytes, just the references.
                      .setData(ByteString.copyFrom(handler.get(key, window)))));
    }

    private static <W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleAppendRequest(
            StateRequest request,
            ByteString key,
            W window,
            BagUserStateHandler<ByteString, ByteString, W> handler) {
      handler.append(key, window, ImmutableList.of(request.getAppend().getData()).iterator());
      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(request.getId())
              .setAppend(StateAppendResponse.getDefaultInstance()));
    }

    private static <W extends BoundedWindow>
        CompletionStage<StateResponse.Builder> handleClearRequest(
            StateRequest request,
            ByteString key,
            W window,
            BagUserStateHandler<ByteString, ByteString, W> handler) {
      handler.clear(key, window);
      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setId(request.getId())
              .setClear(StateClearResponse.getDefaultInstance()));
    }

    private <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> createHandler(
        BagUserStateSpec cacheKey) {
      return handlerFactory.forUserState(
          cacheKey.transformId(),
          cacheKey.userStateId(),
          cacheKey.keyCoder(),
          cacheKey.valueCoder(),
          cacheKey.windowCoder());
    }

    private static BeamFnApi.ProcessBundleRequest.CacheToken createCacheToken() {
      ByteString token = ByteString.copyFrom(UUID.randomUUID().toString().getBytes(Charsets.UTF_8));
      return BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder()
          .setUserState(BeamFnApi.ProcessBundleRequest.CacheToken.UserState.getDefaultInstance())
          .setToken(token)
          .build();
    }
  }
}
