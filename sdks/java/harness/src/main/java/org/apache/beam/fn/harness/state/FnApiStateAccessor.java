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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PCollectionViewTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Provides access to side inputs and state via a {@link BeamFnStateClient}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FnApiStateAccessor<K> implements SideInputReader, StateBinder {

  public interface MutatingStateContext<K, W> {
    K getCurrentKey();

    W getCurrentWindow();
  }

  /**
   * A factory that takes all the immutable parameters to create a {@link FnApiStateAccessor}.
   * Later, once in a particular mutable context, it can create a {@link FnApiStateAccessor}
   * connected to a given mutating context.
   */
  public static class Factory<K> {
    private final PipelineOptions pipelineOptions;
    private final BeamFnStateClient beamFnStateClient;
    private final String ptransformId;
    private final Set<String> runnerCapabilities;
    private final Supplier<String> processBundleInstructionId;
    private final Supplier<List<CacheToken>> cacheTokens;
    private final Supplier<Cache<?, ?>> bundleCache;
    private final Cache<?, ?> processWideCache;
    private final Map<TupleTag<?>, SideInputSpec> sideInputSpecMap;
    private final Coder<K> keyCoder;
    private final Coder<BoundedWindow> windowCoder;

    public Factory(
        PipelineOptions pipelineOptions,
        Set<String> runnerCapabilites,
        String ptransformId,
        Supplier<String> processBundleInstructionId,
        Supplier<List<CacheToken>> cacheTokens,
        Supplier<Cache<?, ?>> bundleCache,
        Cache<?, ?> processWideCache,
        Map<TupleTag<?>, SideInputSpec> sideInputSpecMap,
        BeamFnStateClient beamFnStateClient,
        Coder<K> keyCoder,
        Coder<BoundedWindow> windowCoder) {
      this.pipelineOptions = pipelineOptions;
      this.runnerCapabilities = runnerCapabilites;
      this.ptransformId = ptransformId;
      this.processBundleInstructionId = processBundleInstructionId;
      this.cacheTokens = cacheTokens;
      this.bundleCache = bundleCache;
      this.processWideCache = processWideCache;
      this.sideInputSpecMap = sideInputSpecMap;
      this.beamFnStateClient = beamFnStateClient;
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
    }

    public static <K> Factory<K> factoryForPTransformContext(
        PTransformRunnerFactory.Context context) throws IOException {

      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(context.getComponents())
              .withPipeline(Pipeline.create());
      RunnerApi.ParDoPayload parDoPayload =
          RunnerApi.ParDoPayload.parseFrom(context.getPTransform().getSpec().getPayload());

      @SuppressWarnings("rawtypes") // passed to FnApiStateAccessor which uses rawtypes
      ImmutableMap.Builder<TupleTag<?>, SideInputSpec> tagToSideInputSpecMapBuilder =
          ImmutableMap.builder();
      for (Map.Entry<String, RunnerApi.SideInput> entry :
          parDoPayload.getSideInputsMap().entrySet()) {
        String sideInputTag = entry.getKey();
        RunnerApi.SideInput sideInput = entry.getValue();
        RunnerApi.PCollection sideInputPCollection =
            context
                .getComponents()
                .getPcollectionsMap()
                .get(context.getPTransform().getInputsOrThrow(sideInputTag));
        WindowingStrategy<?, ?> sideInputWindowingStrategy =
            rehydratedComponents.getWindowingStrategy(
                sideInputPCollection.getWindowingStrategyId());
        tagToSideInputSpecMapBuilder.put(
            new TupleTag<>(entry.getKey()),
            SideInputSpec.create(
                sideInput.getAccessPattern().getUrn(),
                rehydratedComponents.getCoder(sideInputPCollection.getCoderId()),
                (Coder<BoundedWindow>) sideInputWindowingStrategy.getWindowFn().windowCoder(),
                PCollectionViewTranslation.viewFnFromProto(entry.getValue().getViewFn()),
                (WindowMappingFn<BoundedWindow>)
                    PCollectionViewTranslation.windowMappingFnFromProto(
                        entry.getValue().getWindowMappingFn())));
      }
      @SuppressWarnings("rawtypes") // passed to FnApiStateAccessor which uses rawtypes
      Map<TupleTag<?>, SideInputSpec> tagToSideInputSpecMap = tagToSideInputSpecMapBuilder.build();

      Coder<K> keyCoder;
      String mainInputTag =
          Iterables.getOnlyElement(
              Sets.difference(
                  context.getPTransform().getInputsMap().keySet(),
                  parDoPayload.getSideInputsMap().keySet()));
      RunnerApi.PCollection mainInput =
          context
              .getComponents()
              .getPcollectionsMap()
              .get(context.getPTransform().getInputsOrThrow(mainInputTag));
      Coder<?> maybeWindowedValueInputCoder = rehydratedComponents.getCoder(mainInput.getCoderId());
      Coder<?> inputCoder;
      if (maybeWindowedValueInputCoder instanceof WindowedValues.WindowedValueCoder) {
        inputCoder =
            ((WindowedValues.WindowedValueCoder<?>) maybeWindowedValueInputCoder).getValueCoder();
      } else {
        inputCoder = maybeWindowedValueInputCoder;
      }
      if (inputCoder instanceof KvCoder) {
        keyCoder = ((KvCoder<K, ?>) inputCoder).getKeyCoder();
      } else {
        keyCoder = null;
      }

      // can get window coder generically
      WindowingStrategy<?, ?> windowingStrategy =
          rehydratedComponents.getWindowingStrategy(mainInput.getWindowingStrategyId());
      Coder<BoundedWindow> windowCoder =
          (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();

      return new Factory<>(
          context.getPipelineOptions(),
          context.getRunnerCapabilities(),
          context.getPTransformId(),
          context.getProcessBundleInstructionIdSupplier(),
          context.getCacheTokensSupplier(),
          context.getBundleCacheSupplier(),
          context.getProcessWideCache(),
          tagToSideInputSpecMap,
          context.getBeamFnStateClient(),
          keyCoder,
          windowCoder);
    }

    public FnApiStateAccessor<K> create() {
      return new FnApiStateAccessor<>(
          pipelineOptions,
          runnerCapabilities,
          ptransformId,
          processBundleInstructionId,
          cacheTokens,
          bundleCache,
          processWideCache,
          sideInputSpecMap,
          beamFnStateClient,
          keyCoder,
          windowCoder);
    }
  }

  private final PipelineOptions pipelineOptions;
  private final Set<String> runnerCapabilites;
  private final Map<StateKey, Object> stateKeyObjectCache;
  private final Map<TupleTag<?>, SideInputSpec> sideInputSpecMap;
  private final BeamFnStateClient beamFnStateClient;
  private final String ptransformId;
  private final Supplier<String> processBundleInstructionId;
  private final Supplier<List<BeamFnApi.ProcessBundleRequest.CacheToken>> cacheTokens;
  private final Supplier<Cache<?, ?>> bundleCache;
  private final Cache<?, ?> processWideCache;
  private final Collection<ThrowingRunnable> stateFinalizers;
  private final Coder<K> keyCoder;
  private final Coder<BoundedWindow> windowCoder;

  private @Nullable Supplier<BoundedWindow> currentWindowSupplier;
  private @Nullable Supplier<ByteString> encodedCurrentKeySupplier;
  private @Nullable Supplier<ByteString> encodedCurrentWindowSupplier;

  public FnApiStateAccessor(
      PipelineOptions pipelineOptions,
      Set<String> runnerCapabilites,
      String ptransformId,
      Supplier<String> processBundleInstructionId,
      Supplier<List<CacheToken>> cacheTokens,
      Supplier<Cache<?, ?>> bundleCache,
      Cache<?, ?> processWideCache,
      Map<TupleTag<?>, SideInputSpec> sideInputSpecMap,
      BeamFnStateClient beamFnStateClient,
      Coder<K> keyCoder,
      Coder<BoundedWindow> windowCoder) {
    this.pipelineOptions = pipelineOptions;
    this.runnerCapabilites = runnerCapabilites;
    this.stateKeyObjectCache = Maps.newHashMap();
    this.sideInputSpecMap = sideInputSpecMap;
    this.beamFnStateClient = beamFnStateClient;
    this.ptransformId = ptransformId;
    this.processBundleInstructionId = processBundleInstructionId;
    this.cacheTokens = cacheTokens;
    this.bundleCache = bundleCache;
    this.processWideCache = processWideCache;
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
    this.stateFinalizers = new ArrayList<>();
  }

  public void setKeyAndWindowContext(MutatingStateContext<K, BoundedWindow> keyAndWindowContext) {
    this.currentWindowSupplier = keyAndWindowContext::getCurrentWindow;
    this.encodedCurrentKeySupplier =
        memoizeFunction(
            keyAndWindowContext::getCurrentKey,
            key -> {
              checkState(
                  keyCoder != null, "Accessing state in unkeyed context, no key coder available");

              ByteStringOutputStream encodedKeyOut = new ByteStringOutputStream();
              try {
                ((Coder) keyCoder).encode(key, encodedKeyOut, Coder.Context.NESTED);
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
              return encodedKeyOut.toByteString();
            });

    this.encodedCurrentWindowSupplier =
        memoizeFunction(
            currentWindowSupplier,
            window -> {
              ByteStringOutputStream encodedWindowOut = new ByteStringOutputStream();
              try {
                windowCoder.encode(window, encodedWindowOut);
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
              return encodedWindowOut.toByteString();
            });
  }

  private static <ArgT, ResultT> Supplier<ResultT> memoizeFunction(
      Supplier<ArgT> arg, Function<ArgT, ResultT> f) {
    return new Supplier<ResultT>() {
      private ArgT memoizedArg;
      private ResultT memoizedResult;
      private boolean initialized;

      @Override
      public ResultT get() {
        ArgT currentArg = arg.get();
        if (currentArg != memoizedArg || !initialized) {
          memoizedResult = f.apply(this.memoizedArg = currentArg);
          initialized = true;
        }
        return memoizedResult;
      }
    };
  }

  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    TupleTag<?> tag = view.getTagInternal();

    SideInputSpec sideInputSpec = sideInputSpecMap.get(tag);
    checkArgument(sideInputSpec != null, "Attempting to access unknown side input %s.", view);

    ByteStringOutputStream encodedWindowOut = new ByteStringOutputStream();
    try {
      sideInputSpec
          .getWindowCoder()
          .encode(sideInputSpec.getWindowMappingFn().getSideInputWindow(window), encodedWindowOut);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    ByteString encodedWindow = encodedWindowOut.toByteString();
    // IDEA: If this StateKey shows up on caching profiles, create a custom object that is cheap to
    // weigh and compare similar to the other statekey types in this file.
    StateKey.Builder cacheKeyBuilder = StateKey.newBuilder();

    switch (sideInputSpec.getAccessPattern()) {
      case Materializations.ITERABLE_MATERIALIZATION_URN:
        cacheKeyBuilder
            .getIterableSideInputBuilder()
            .setTransformId(ptransformId)
            .setSideInputId(tag.getId())
            .setWindow(encodedWindow);
        break;

      case Materializations.MULTIMAP_MATERIALIZATION_URN:
        checkState(
            sideInputSpec.getCoder() instanceof KvCoder,
            "Expected %s but received %s.",
            KvCoder.class,
            sideInputSpec.getCoder().getClass());
        cacheKeyBuilder
            .getMultimapKeysSideInputBuilder()
            .setTransformId(ptransformId)
            .setSideInputId(tag.getId())
            .setWindow(encodedWindow);
        break;

      default:
        throw new IllegalStateException(
            String.format(
                "This SDK is only capable of dealing with %s materializations "
                    + "but was asked to handle %s for PCollectionView with tag %s.",
                ImmutableList.of(
                    Materializations.ITERABLE_MATERIALIZATION_URN,
                    Materializations.MULTIMAP_MATERIALIZATION_URN),
                sideInputSpec.getAccessPattern(),
                tag));
    }
    return (T)
        stateKeyObjectCache.computeIfAbsent(
            cacheKeyBuilder.build(),
            key -> {
              switch (sideInputSpec.getAccessPattern()) {
                case Materializations.ITERABLE_MATERIALIZATION_URN:
                  return sideInputSpec
                      .getViewFn()
                      .apply(
                          new IterableSideInput<>(
                              getCacheFor(key),
                              beamFnStateClient,
                              processBundleInstructionId.get(),
                              key,
                              sideInputSpec.getCoder()));
                case Materializations.MULTIMAP_MATERIALIZATION_URN:
                  return sideInputSpec
                      .getViewFn()
                      .apply(
                          new MultimapSideInput<>(
                              getCacheFor(key),
                              beamFnStateClient,
                              processBundleInstructionId.get(),
                              key,
                              ((KvCoder) sideInputSpec.getCoder()).getKeyCoder(),
                              ((KvCoder) sideInputSpec.getCoder()).getValueCoder(),
                              runnerCapabilites.contains(
                                  BeamUrns.getUrn(
                                      RunnerApi.StandardRunnerProtocols.Enum
                                          .MULTIMAP_KEYS_VALUES_SIDE_INPUT))));
                default:
                  throw new IllegalStateException(
                      String.format(
                          "This SDK is only capable of dealing with %s materializations "
                              + "but was asked to handle %s for PCollectionView with tag %s.",
                          ImmutableList.of(
                              Materializations.ITERABLE_MATERIALIZATION_URN,
                              Materializations.MULTIMAP_MATERIALIZATION_URN),
                          sideInputSpec.getAccessPattern(),
                          tag));
              }
            });
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputSpecMap.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputSpecMap.isEmpty();
  }

  @Override
  public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
    return (ValueState<T>)
        stateKeyObjectCache.computeIfAbsent(
            createBagUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                return new ValueState<T>() {
                  private final BagUserState<T> impl = createBagUserState(key, coder);

                  @Override
                  public void clear() {
                    impl.clear();
                  }

                  @Override
                  public void write(T input) {
                    impl.clear();
                    impl.append(input);
                  }

                  @Override
                  public T read() {
                    Iterator<T> value = impl.get().iterator();
                    if (value.hasNext()) {
                      return value.next();
                    } else {
                      return null;
                    }
                  }

                  @Override
                  public ValueState<T> readLater() {
                    impl.get().prefetch();
                    return this;
                  }
                };
              }
            });
  }

  @Override
  public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
    return (BagState<T>)
        stateKeyObjectCache.computeIfAbsent(
            createBagUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                return new BagState<T>() {
                  private final BagUserState<T> impl = createBagUserState(key, elemCoder);

                  @Override
                  public void add(T value) {
                    impl.append(value);
                  }

                  @Override
                  public ReadableState<Boolean> isEmpty() {
                    return new ReadableState<Boolean>() {
                      @Override
                      public @Nullable Boolean read() {
                        return !impl.get().iterator().hasNext();
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        return this;
                      }
                    };
                  }

                  @Override
                  public Iterable<T> read() {
                    return impl.get();
                  }

                  @Override
                  public BagState<T> readLater() {
                    impl.get().prefetch();
                    return this;
                  }

                  @Override
                  public void clear() {
                    impl.clear();
                  }
                };
              }
            });
  }

  @Override
  public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
    return (SetState<T>)
        stateKeyObjectCache.computeIfAbsent(
            createMultimapKeysUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                return new SetState<T>() {
                  private final MultimapUserState<T, Void> impl =
                      createMultimapUserState(key, elemCoder, VoidCoder.of());

                  @Override
                  public void clear() {
                    impl.clear();
                  }

                  @Override
                  public ReadableState<Boolean> contains(T t) {
                    return new ReadableState<Boolean>() {
                      @Override
                      public Boolean read() {
                        return !Iterables.isEmpty(impl.get(t));
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        impl.get(t).prefetch();
                        return this;
                      }
                    };
                  }

                  @Override
                  public ReadableState<Boolean> addIfAbsent(T t) {
                    boolean isEmpty = Iterables.isEmpty(impl.get(t));
                    if (isEmpty) {
                      impl.put(t, null);
                    }
                    return ReadableStates.immediate(isEmpty);
                  }

                  @Override
                  public void remove(T t) {
                    impl.remove(t);
                  }

                  @Override
                  public void add(T value) {
                    impl.remove(value);
                    impl.put(value, null);
                  }

                  @Override
                  public ReadableState<Boolean> isEmpty() {
                    return new ReadableState<Boolean>() {
                      @Override
                      public Boolean read() {
                        return Iterables.isEmpty(impl.keys());
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        impl.keys().prefetch();
                        return this;
                      }
                    };
                  }

                  @Override
                  public Iterable<T> read() {
                    return impl.keys();
                  }

                  @Override
                  public SetState<T> readLater() {
                    impl.keys().prefetch();
                    return this;
                  }
                };
              }
            });
  }

  @Override
  public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
      String id,
      StateSpec<MapState<KeyT, ValueT>> spec,
      Coder<KeyT> mapKeyCoder,
      Coder<ValueT> mapValueCoder) {
    return (MapState<KeyT, ValueT>)
        stateKeyObjectCache.computeIfAbsent(
            createMultimapKeysUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                return new MapState<KeyT, ValueT>() {
                  private final MultimapUserState<KeyT, ValueT> impl =
                      createMultimapUserState(key, mapKeyCoder, mapValueCoder);

                  @Override
                  public void clear() {
                    impl.clear();
                  }

                  @Override
                  public void put(KeyT key, ValueT value) {
                    impl.remove(key);
                    impl.put(key, value);
                  }

                  @Override
                  public ReadableState<ValueT> computeIfAbsent(
                      KeyT key, Function<? super KeyT, ? extends ValueT> mappingFunction) {
                    Iterable<ValueT> values = impl.get(key);
                    if (Iterables.isEmpty(values)) {
                      impl.put(key, mappingFunction.apply(key));
                    }
                    return ReadableStates.immediate(Iterables.getOnlyElement(values, null));
                  }

                  @Override
                  public void remove(KeyT key) {
                    impl.remove(key);
                  }

                  @Override
                  public ReadableState<ValueT> get(KeyT key) {
                    return getOrDefault(key, null);
                  }

                  @Override
                  public ReadableState<ValueT> getOrDefault(
                      KeyT key, @Nullable ValueT defaultValue) {
                    return new ReadableState<ValueT>() {
                      @Override
                      public @Nullable ValueT read() {
                        Iterable<ValueT> values = impl.get(key);
                        return Iterables.getOnlyElement(values, defaultValue);
                      }

                      @Override
                      public ReadableState<ValueT> readLater() {
                        impl.get(key).prefetch();
                        return this;
                      }
                    };
                  }

                  @Override
                  public ReadableState<Iterable<KeyT>> keys() {
                    return new ReadableState<Iterable<KeyT>>() {
                      @Override
                      public Iterable<KeyT> read() {
                        return impl.keys();
                      }

                      @Override
                      public ReadableState<Iterable<KeyT>> readLater() {
                        impl.keys().prefetch();
                        return this;
                      }
                    };
                  }

                  @Override
                  public ReadableState<Iterable<ValueT>> values() {
                    return new ReadableState<Iterable<ValueT>>() {
                      @Override
                      public Iterable<ValueT> read() {
                        return Iterables.transform(entries().read(), e -> e.getValue());
                      }

                      @Override
                      public ReadableState<Iterable<ValueT>> readLater() {
                        entries().readLater();
                        return this;
                      }
                    };
                  }

                  @Override
                  public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> entries() {
                    return new ReadableState<Iterable<Map.Entry<KeyT, ValueT>>>() {
                      @Override
                      public Iterable<Map.Entry<KeyT, ValueT>> read() {
                        Iterable<KeyT> keys = keys().read();
                        return Iterables.transform(
                            keys, key -> Maps.immutableEntry(key, get(key).read()));
                      }

                      @Override
                      public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> readLater() {
                        // Start prefetching the keys. We would need to block to start prefetching
                        // the values.
                        keys().readLater();
                        return this;
                      }
                    };
                  }

                  @Override
                  public ReadableState<Boolean> isEmpty() {
                    return new ReadableState<Boolean>() {
                      @Override
                      public Boolean read() {
                        return Iterables.isEmpty(keys().read());
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        keys().readLater();
                        return this;
                      }
                    };
                  }
                };
              }
            });
  }

  @Override
  public <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
      String id,
      StateSpec<MultimapState<KeyT, ValueT>> spec,
      Coder<KeyT> keyCoder,
      Coder<ValueT> valueCoder) {
    // TODO(https://github.com/apache/beam/issues/23616)
    throw new UnsupportedOperationException("Multimap is not currently supported with Fn API.");
  }

  @Override
  public <T> OrderedListState<T> bindOrderedList(
      String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
    return (OrderedListState<T>)
        stateKeyObjectCache.computeIfAbsent(
            createOrderedListUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                return new OrderedListState<T>() {
                  private final OrderedListUserState<T> impl =
                      createOrderedListUserState(key, elemCoder);

                  @Override
                  public void clear() {
                    impl.clear();
                  }

                  @Override
                  public void add(TimestampedValue<T> value) {
                    impl.add(value);
                  }

                  @Override
                  public ReadableState<Boolean> isEmpty() {
                    return new ReadableState<Boolean>() {
                      @Override
                      public @Nullable Boolean read() {
                        return !impl.read().iterator().hasNext();
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        return this;
                      }
                    };
                  }

                  @Nullable
                  @Override
                  public Iterable<TimestampedValue<T>> read() {
                    return readRange(
                        Instant.ofEpochMilli(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE));
                  }

                  @Override
                  public GroupingState<TimestampedValue<T>, Iterable<TimestampedValue<T>>>
                      readLater() {
                    return this;
                  }

                  @Override
                  public Iterable<TimestampedValue<T>> readRange(
                      Instant minTimestamp, Instant limitTimestamp) {
                    return impl.readRange(minTimestamp, limitTimestamp);
                  }

                  @Override
                  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
                    impl.clearRange(minTimestamp, limitTimestamp);
                  }

                  @Override
                  public OrderedListState<T> readRangeLater(
                      Instant minTimestamp, Instant limitTimestamp) {
                    return this;
                  }
                };
              }
            });
  }

  @Override
  public <ElementT, AccumT, ResultT> CombiningState<ElementT, AccumT, ResultT> bindCombining(
      String id,
      StateSpec<CombiningState<ElementT, AccumT, ResultT>> spec,
      Coder<AccumT> accumCoder,
      CombineFn<ElementT, AccumT, ResultT> combineFn) {
    return (CombiningState<ElementT, AccumT, ResultT>)
        stateKeyObjectCache.computeIfAbsent(
            createBagUserStateKey(id),
            new Function<StateKey, Object>() {
              @Override
              public Object apply(StateKey key) {
                // TODO: Support squashing accumulators depending on whether we know of all
                // remote accumulators and local accumulators or just local accumulators.
                return new CombiningState<ElementT, AccumT, ResultT>() {
                  private final BagUserState<AccumT> impl = createBagUserState(key, accumCoder);

                  @Override
                  public AccumT getAccum() {
                    Iterator<AccumT> iterator = impl.get().iterator();
                    if (iterator.hasNext()) {
                      return iterator.next();
                    }
                    return combineFn.createAccumulator();
                  }

                  @Override
                  public void addAccum(AccumT accum) {
                    Iterator<AccumT> iterator = impl.get().iterator();

                    // Only merge if there was a prior value
                    if (iterator.hasNext()) {
                      accum = combineFn.mergeAccumulators(ImmutableList.of(iterator.next(), accum));
                      // Since there was a prior value, we need to clear.
                      impl.clear();
                    }

                    impl.append(accum);
                  }

                  @Override
                  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
                    return combineFn.mergeAccumulators(accumulators);
                  }

                  @Override
                  public CombiningState<ElementT, AccumT, ResultT> readLater() {
                    impl.get().prefetch();
                    return this;
                  }

                  @Override
                  public ResultT read() {
                    Iterator<AccumT> iterator = impl.get().iterator();
                    if (iterator.hasNext()) {
                      return combineFn.extractOutput(iterator.next());
                    }
                    return combineFn.defaultValue();
                  }

                  @Override
                  public void add(ElementT value) {
                    AccumT newAccumulator = combineFn.addInput(getAccum(), value);
                    impl.clear();
                    impl.append(newAccumulator);
                  }

                  @Override
                  public ReadableState<Boolean> isEmpty() {
                    return new ReadableState<Boolean>() {
                      @Override
                      public @Nullable Boolean read() {
                        return !impl.get().iterator().hasNext();
                      }

                      @Override
                      public ReadableState<Boolean> readLater() {
                        impl.get().prefetch();
                        return this;
                      }
                    };
                  }

                  @Override
                  public void clear() {
                    impl.clear();
                  }
                };
              }
            });
  }

  @Override
  public <ElementT, AccumT, ResultT>
      CombiningState<ElementT, AccumT, ResultT> bindCombiningWithContext(
          String id,
          StateSpec<CombiningState<ElementT, AccumT, ResultT>> spec,
          Coder<AccumT> accumCoder,
          CombineFnWithContext<ElementT, AccumT, ResultT> combineFn) {
    return (CombiningState<ElementT, AccumT, ResultT>)
        stateKeyObjectCache.computeIfAbsent(
            createBagUserStateKey(id),
            key ->
                bindCombining(
                    id,
                    spec,
                    accumCoder,
                    CombineFnUtil.bindContext(
                        combineFn,
                        new StateContext<BoundedWindow>() {
                          @Override
                          public PipelineOptions getPipelineOptions() {
                            return pipelineOptions;
                          }

                          @Override
                          public <T> T sideInput(PCollectionView<T> view) {
                            return get(view, currentWindowSupplier.get());
                          }

                          @Override
                          public BoundedWindow window() {
                            return currentWindowSupplier.get();
                          }
                        })));
  }

  /**
   * @deprecated The Fn API has no plans to implement WatermarkHoldState as of this writing and is
   *     waiting on resolution of BEAM-2535.
   */
  @Override
  @Deprecated
  public WatermarkHoldState bindWatermark(
      String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
    throw new UnsupportedOperationException("WatermarkHoldState is unsupported by the Fn API.");
  }

  private static class UserStateCacheTokenKey implements Weighted {
    private final ByteString bytes;
    private final int hash;

    public UserStateCacheTokenKey(ByteString bytes) {
      this.bytes = bytes;
      this.hash = Objects.hash(UserStateCacheTokenKey.class, bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof UserStateCacheTokenKey)) {
        return false;
      }
      UserStateCacheTokenKey other = (UserStateCacheTokenKey) o;
      return hash == other.hash && bytes.equals(other.bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public long getWeight() {
      // 12 = 4 bytes for int + 8 for reference.
      // This doesn't account for backing memory of bytes but reducing
      // overhead of weighing is more important.
      return 12L + bytes.size();
    }
  }

  private Cache<?, ?> getCacheFor(StateKey stateKey) {
    switch (stateKey.getTypeCase()) {
      case BAG_USER_STATE:
        for (CacheToken token : cacheTokens.get()) {
          if (!token.hasUserState()) {
            continue;
          }
          return Caches.subCache(
              processWideCache,
              new UserStateCacheTokenKey(token.getToken()),
              new BagUserStateCacheKey(stateKey.getBagUserState()));
        }
        break;
      case MULTIMAP_KEYS_USER_STATE:
        for (CacheToken token : cacheTokens.get()) {
          if (!token.hasUserState()) {
            continue;
          }
          return Caches.subCache(
              processWideCache,
              new UserStateCacheTokenKey(token.getToken()),
              new MultimapKeysUserStateCacheKey(stateKey.getMultimapKeysUserState()));
        }
        break;
      case ORDERED_LIST_USER_STATE:
        for (CacheToken token : cacheTokens.get()) {
          if (!token.hasUserState()) {
            continue;
          }
          return Caches.subCache(
              processWideCache,
              new UserStateCacheTokenKey(token.getToken()),
              new OrderedListUserStateCacheKey(stateKey.getOrderedListUserState()));
        }
        break;
      case ITERABLE_SIDE_INPUT:
        for (CacheToken token : cacheTokens.get()) {
          if (!token.hasSideInput()) {
            continue;
          }
          if (stateKey
                  .getIterableSideInput()
                  .getTransformId()
                  .equals(token.getSideInput().getTransformId())
              && stateKey
                  .getIterableSideInput()
                  .getSideInputId()
                  .equals(token.getSideInput().getSideInputId())) {
            // IDEA: If cachetoken shows up on profiles, create a simpler type to weigh like
            // UserStateCacheTokenKey.
            return Caches.subCache(processWideCache, token, stateKey);
          }
        }
        break;
      case MULTIMAP_KEYS_SIDE_INPUT:
        for (CacheToken token : cacheTokens.get()) {
          if (!token.hasSideInput()) {
            continue;
          }
          if (stateKey
                  .getMultimapKeysSideInput()
                  .getTransformId()
                  .equals(token.getSideInput().getTransformId())
              && stateKey
                  .getMultimapKeysSideInput()
                  .getSideInputId()
                  .equals(token.getSideInput().getSideInputId())) {
            // IDEA: If cachetoken shows up on profiles, create a simpler type to weigh like
            // UserStateCacheTokenKey.
            return Caches.subCache(processWideCache, token, stateKey);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            String.format("Unknown state key type requested %s.", stateKey));
    }
    // The default is to use the bundle cache.
    return Caches.subCache(bundleCache.get(), stateKey);
  }

  private <T> BagUserState<T> createBagUserState(StateKey stateKey, Coder<T> valueCoder) {
    BagUserState<T> rval =
        new BagUserState<>(
            getCacheFor(stateKey),
            beamFnStateClient,
            processBundleInstructionId.get(),
            stateKey,
            valueCoder);
    stateFinalizers.add(rval::asyncClose);
    return rval;
  }

  // Shared base for implementation of cache keys with the same
  // fields that also uses the subclass for hashing and equality.
  private abstract static class UserStateCacheKeyBase implements Weighted {
    private final String ptransformId;
    private final String stateId;
    private final ByteString window;
    private final ByteString key;
    private final int hash;

    protected UserStateCacheKeyBase(
        Class subclass, String ptransformId, String stateId, ByteString window, ByteString key) {
      this.ptransformId = ptransformId;
      this.stateId = stateId;
      this.window = window;
      this.key = key;
      this.hash = Objects.hash(subclass, ptransformId, stateId, window, key);
    }

    @Override
    public final boolean equals(Object o) {
      if (!(o instanceof UserStateCacheKeyBase)) {
        return false;
      }
      UserStateCacheKeyBase other = (UserStateCacheKeyBase) o;
      return hash == other.hash
          && this.getClass().equals(o.getClass())
          && ptransformId.equals(other.ptransformId)
          && stateId.equals(other.stateId)
          && window.equals(other.window)
          && key.equals(other.key);
    }

    @Override
    public final int hashCode() {
      return hash;
    }

    @Override
    public final long getWeight() {
      // 36 = 4 bytes for int + 8 * 4 references.
      // This doesn't account for backing memory of bytes but reducing
      // overhead of weighing is more important.
      return 36L + ptransformId.length() + stateId.length() + window.size() + key.size();
    }
  }

  private static final class BagUserStateCacheKey extends UserStateCacheKeyBase {
    public BagUserStateCacheKey(StateKey.BagUserState proto) {
      super(
          BagUserStateCacheKey.class,
          proto.getTransformId(),
          proto.getUserStateId(),
          proto.getWindow(),
          proto.getKey());
    }
  }

  private StateKey createBagUserStateKey(String stateId) {
    StateKey.Builder builder = StateKey.newBuilder();
    builder
        .getBagUserStateBuilder()
        .setWindow(encodedCurrentWindowSupplier.get())
        .setKey(encodedCurrentKeySupplier.get())
        .setTransformId(ptransformId)
        .setUserStateId(stateId);
    return builder.build();
  }

  private <KeyT, ValueT> MultimapUserState<KeyT, ValueT> createMultimapUserState(
      StateKey stateKey, Coder<KeyT> keyCoder, Coder<ValueT> valueCoder) {
    MultimapUserState<KeyT, ValueT> rval =
        new MultimapUserState(
            getCacheFor(stateKey),
            beamFnStateClient,
            processBundleInstructionId.get(),
            stateKey,
            keyCoder,
            valueCoder);
    stateFinalizers.add(rval::asyncClose);
    return rval;
  }

  private static final class MultimapKeysUserStateCacheKey extends UserStateCacheKeyBase {
    public MultimapKeysUserStateCacheKey(StateKey.MultimapKeysUserState proto) {
      super(
          MultimapKeysUserStateCacheKey.class,
          proto.getTransformId(),
          proto.getUserStateId(),
          proto.getWindow(),
          proto.getKey());
    }
  }

  private StateKey createMultimapKeysUserStateKey(String stateId) {
    StateKey.Builder builder = StateKey.newBuilder();
    builder
        .getMultimapKeysUserStateBuilder()
        .setWindow(encodedCurrentWindowSupplier.get())
        .setKey(encodedCurrentKeySupplier.get())
        .setTransformId(ptransformId)
        .setUserStateId(stateId);
    return builder.build();
  }

  private <T> OrderedListUserState<T> createOrderedListUserState(
      StateKey stateKey, Coder<T> valueCoder) {
    OrderedListUserState<T> rval =
        new OrderedListUserState<>(
            getCacheFor(stateKey),
            beamFnStateClient,
            processBundleInstructionId.get(),
            stateKey,
            valueCoder);
    stateFinalizers.add(rval::asyncClose);
    return rval;
  }

  private static final class OrderedListUserStateCacheKey extends UserStateCacheKeyBase {
    public OrderedListUserStateCacheKey(StateKey.OrderedListUserState proto) {
      super(
          OrderedListUserStateCacheKey.class,
          proto.getTransformId(),
          proto.getUserStateId(),
          proto.getWindow(),
          proto.getKey());
    }
  }

  private StateKey createOrderedListUserStateKey(String stateId) {
    StateKey.Builder builder = StateKey.newBuilder();
    builder
        .getOrderedListUserStateBuilder()
        .setWindow(encodedCurrentWindowSupplier.get())
        .setKey(encodedCurrentKeySupplier.get())
        .setTransformId(ptransformId)
        .setUserStateId(stateId);
    return builder.build();
  }

  public void finalizeState() {
    // Persist all dirty state cells
    try {
      for (ThrowingRunnable runnable : stateFinalizers) {
        runnable.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    stateFinalizers.clear();
    stateKeyObjectCache.clear();
  }
}
