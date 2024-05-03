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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Source;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews.HasDefaultValue;
import org.apache.beam.sdk.values.PCollectionViews.IterableBackedListViewFn;
import org.apache.beam.sdk.values.PCollectionViews.IterableViewFn;
import org.apache.beam.sdk.values.PCollectionViews.IterableViewFn2;
import org.apache.beam.sdk.values.PCollectionViews.ListViewFn;
import org.apache.beam.sdk.values.PCollectionViews.ListViewFn2;
import org.apache.beam.sdk.values.PCollectionViews.MapViewFn;
import org.apache.beam.sdk.values.PCollectionViews.MapViewFn2;
import org.apache.beam.sdk.values.PCollectionViews.MultimapViewFn;
import org.apache.beam.sdk.values.PCollectionViews.MultimapViewFn2;
import org.apache.beam.sdk.values.PCollectionViews.SingletonViewFn;
import org.apache.beam.sdk.values.PCollectionViews.SingletonViewFn2;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A side input reader over a set of {@link IsmFormat} files constructed by Dataflow. This reader
 * expects a very specific key and metadata structure within the files for each side input type. See
 * {@link #getSingletonForWindow} for singleton views, {@link #getListForWindow} for iterable and
 * list views, and {@link #getMapForWindow} for map and multimap views.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
  "nullness"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class IsmSideInputReader implements SideInputReader {
  private static final String SINGLETON_KIND = "singleton";
  private static final String COLLECTION_KIND = "collection";
  private static final Object NULL_PLACE_HOLDER = new Object();

  private static final ImmutableList<Class<? extends ViewFn>> KNOWN_SINGLETON_VIEW_TYPES =
      ImmutableList.of(
          SingletonViewFn.class,
          SingletonViewFn2.class,
          MapViewFn.class,
          MapViewFn2.class,
          MultimapViewFn.class,
          MultimapViewFn2.class);

  /**
   * Limit the number of concurrent initializations.
   *
   * <p>TODO: Move the bounded executor out to an accessible place such as on PipelineOptions.
   */
  private static final ExecutorService SHARED_EXECUTOR_SERVICE =
      Executors.newFixedThreadPool(64 /* concurrency limit */);

  private final ExecutorService executorService;
  private final Set<TupleTag<?>> singletonMaterializedTags;
  /** A map from tuple tag to non-empty IsmReaders. */
  @VisibleForTesting final Map<TupleTag<?>, List<IsmReader<?>>> tagToIsmReaderMap;
  /**
   * A map from tuple tag to empty IsmReaders. Even though this is unused, we want to maintain a
   * strong reference so that it is retained in memory so the logical reference cache keeps a
   * reference so that other IsmSideInputReader instances will not need to continuously initialize
   * empty IsmReaders.
   */
  @SuppressWarnings("unused")
  @VisibleForTesting
  final Map<TupleTag<?>, List<IsmReader<?>>> tagToEmptyIsmReaderMap;

  private final ReaderFactory readerFactory;
  private final BatchModeExecutionContext executionContext;
  private final DataflowOperationContext operationContext;

  private IsmSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos,
      PipelineOptions options,
      BatchModeExecutionContext executionContext,
      ReaderFactory readerFactory,
      DataflowOperationContext operationContext,
      ExecutorService executorService)
      throws Exception {
    this.readerFactory = readerFactory;
    this.executionContext = executionContext;
    this.operationContext = operationContext;
    this.executorService = executorService;

    Map<TupleTag<?>, List<IsmReader<?>>> tagToAllIsmReaders = new HashMap<>();
    int sideInputIndex = 1;
    for (SideInputInfo sideInputInfo : sideInputInfos) {
      TupleTag<?> tag = new TupleTag<>(sideInputInfo.getTag());
      List<IsmReader<?>> readers =
          createReadersFromSources(options, sideInputInfo, executionContext, sideInputIndex);
      tagToAllIsmReaders.put(tag, readers);
      sideInputIndex++;
    }

    // Collect all the IsmReaders that need to be initialized
    List<Callable<Void>> ismReadersToInitialize = new ArrayList<>();
    for (final IsmReader<?> ismReader : Iterables.concat(tagToAllIsmReaders.values())) {
      if (!ismReader.isInitialized()) {
        ismReadersToInitialize.add(
            () -> {
              // Checking whether the file is empty ensures that the footer is loaded.
              ismReader.isEmpty();
              return null;
            });
      }
    }
    // Initialize all the IsmReaders in parallel and wait.
    List<Future<Void>> initializations = executorService.invokeAll(ismReadersToInitialize);
    for (Future<Void> initialization : initializations) {
      initialization.get();
    }

    ImmutableMap.Builder<TupleTag<?>, List<IsmReader<?>>> tagToIsmReaderMapBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<TupleTag<?>, List<IsmReader<?>>> tagToEmptyIsmReaderMapBuilder =
        ImmutableMap.builder();
    ImmutableSet.Builder<TupleTag<?>> singletonMaterializationBuilder = ImmutableSet.builder();
    for (Map.Entry<TupleTag<?>, List<IsmReader<?>>> entry : tagToAllIsmReaders.entrySet()) {
      ImmutableList.Builder<IsmReader<?>> nonEmptyIsmReaders = ImmutableList.builder();
      ImmutableList.Builder<IsmReader<?>> emptyIsmReaders = ImmutableList.builder();
      for (IsmReader<?> ismReader : entry.getValue()) {
        if (ismReader.isEmpty()) {
          emptyIsmReaders.add(ismReader);
        } else {
          if (ismReader.getCoder().getKeyComponentCoders().size() == 1) {
            // The coder for a singleton-materialized Side Input will contain only the window in the
            // key. Iterable/List materializations have Window + Index, and Multimap has
            // Window + Key + Index
            singletonMaterializationBuilder.add(entry.getKey());
          }
          nonEmptyIsmReaders.add(ismReader);
        }
      }
      tagToIsmReaderMapBuilder.put(entry.getKey(), nonEmptyIsmReaders.build());
      tagToEmptyIsmReaderMapBuilder.put(entry.getKey(), emptyIsmReaders.build());
    }
    this.tagToIsmReaderMap = tagToIsmReaderMapBuilder.build();
    this.tagToEmptyIsmReaderMap = tagToEmptyIsmReaderMapBuilder.build();
    this.singletonMaterializedTags = singletonMaterializationBuilder.build();
  }

  private List<IsmReader<?>> createReadersFromSources(
      PipelineOptions options,
      SideInputInfo sideInputInfo,
      DataflowExecutionContext executionContext,
      int sideInputIndex)
      throws Exception {
    String sideInputKind = getString(sideInputInfo.getKind(), PropertyNames.OBJECT_TYPE_NAME);
    if (SINGLETON_KIND.equals(sideInputKind)) {
      checkState(
          sideInputInfo.getSources().size() == 1,
          "expecting a singleton side input kind to have a single source");
    } else if (!COLLECTION_KIND.equals(sideInputKind)) {
      throw new Exception("unexpected kind of side input: " + sideInputKind);
    }

    SideInputReadCounter sideInputReadCounter =
        new DataflowSideInputReadCounter(executionContext, operationContext, sideInputIndex);

    ImmutableList.Builder<IsmReader<?>> builder = ImmutableList.builder();
    for (Source source : sideInputInfo.getSources()) {
      Coder<?> coder = null;
      if (source.getCodec() != null) {
        coder = CloudObjects.coderFromCloudObject(CloudObject.fromSpec(source.getCodec()));
      }

      CloudObject spec = CloudObject.fromSpec(source.getSpec());
      final String filepattern = getString(spec, WorkerPropertyNames.FILENAME);

      for (String file : Filepatterns.expandAtNFilepattern(filepattern)) {
        CloudObject fileSpec = spec.clone(); // Deep clone.
        addString(fileSpec, WorkerPropertyNames.FILENAME, file);

        @SuppressWarnings("unchecked")
        NativeReader<?> reader =
            readerFactory.create(fileSpec, coder, options, executionContext, operationContext);

        checkState(
            reader instanceof IsmReader,
            "%s only supports %s as a reader but was %s.",
            IsmSideInputReader.class.getSimpleName(),
            IsmReader.class.getSimpleName(),
            reader.getClass().getSimpleName());

        IsmReader ismReader = (IsmReader) reader;
        builder.add(new SideInputTrackingIsmReader<>(ismReader, sideInputReadCounter));
      }
    }
    return builder.build();
  }

  /**
   * Creates a new {@link SideInputReader} that will provide side inputs according to the provided
   * {@link SideInputInfo} descriptors.
   */
  static IsmSideInputReader of(
      Iterable<? extends SideInputInfo> sideInputInfos,
      PipelineOptions options,
      BatchModeExecutionContext context,
      ReaderFactory readerFactory,
      DataflowOperationContext operationContext)
      throws Exception {
    return new IsmSideInputReader(
        sideInputInfos, options, context, readerFactory, operationContext, SHARED_EXECUTOR_SERVICE);
  }

  static IsmSideInputReader forTest(
      Iterable<? extends SideInputInfo> sideInputInfos,
      PipelineOptions options,
      BatchModeExecutionContext context,
      ReaderFactory readerFactory,
      DataflowOperationContext operationContext,
      ExecutorService testExecutorService)
      throws Exception {
    return new IsmSideInputReader(
        sideInputInfos, options, context, readerFactory, operationContext, testExecutorService);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return tagToIsmReaderMap.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return tagToIsmReaderMap.isEmpty();
  }

  @Override
  public <ViewT> ViewT get(final PCollectionView<ViewT> view, final BoundedWindow window) {
    @SuppressWarnings("rawtypes")
    final TupleTag tag = view.getTagInternal();
    checkArgument(tagToIsmReaderMap.containsKey(tag), "calling getSideInput() with unknown view");

    // We specialize each individual view with a specific data structure tailored
    // for its use.
    try {
      ViewFn<?, ?> viewFn = view.getViewFn();
      // We handle the singleton case separately since a null value may be returned.
      // We use a null place holder to represent this, and when we detect it, we translate
      // back to null for the user.
      if (viewFn instanceof SingletonViewFn || viewFn instanceof SingletonViewFn2) {
        ViewT rval =
            executionContext
                .<PCollectionViewWindow<ViewT>, ViewT>getLogicalReferenceCache()
                .get(
                    PCollectionViewWindow.of(view, window),
                    () -> {
                      @SuppressWarnings("unchecked")
                      ViewT viewT =
                          getSingletonForWindow(tag, (HasDefaultValue<ViewT>) viewFn, window);
                      @SuppressWarnings("unchecked")
                      ViewT nullPlaceHolder = (ViewT) NULL_PLACE_HOLDER;
                      return viewT == null ? nullPlaceHolder : viewT;
                    });
        return rval == NULL_PLACE_HOLDER ? null : rval;
      } else if (singletonMaterializedTags.contains(tag)) {
        checkArgument(
            viewFn instanceof MapViewFn
                || viewFn instanceof MapViewFn2
                || viewFn instanceof MultimapViewFn
                || viewFn instanceof MultimapViewFn2,
            "Unknown view type stored as singleton. Expected one of %s, got %s",
            KNOWN_SINGLETON_VIEW_TYPES,
            viewFn.getClass().getName());
        return executionContext
            .<PCollectionViewWindow<ViewT>, ViewT>getLogicalReferenceCache()
            .get(
                PCollectionViewWindow.of(view, window),
                () -> {
                  return getMapSingletonForViewAndWindow(tag, window);
                });
      } else {
        return executionContext
            .<PCollectionViewWindow<ViewT>, ViewT>getLogicalReferenceCache()
            .get(
                PCollectionViewWindow.of(view, window),
                () -> {
                  if (viewFn instanceof IterableViewFn
                      || viewFn instanceof IterableViewFn2
                      || viewFn instanceof ListViewFn
                      || viewFn instanceof ListViewFn2
                      || viewFn instanceof IterableBackedListViewFn) {
                    @SuppressWarnings("unchecked")
                    ViewT viewT = (ViewT) getListForWindow(tag, window);
                    return viewT;
                  } else if (viewFn instanceof MapViewFn || viewFn instanceof MapViewFn2) {
                    @SuppressWarnings("unchecked")
                    ViewT viewT = (ViewT) getMapForWindow(tag, window);
                    return viewT;
                  } else if (viewFn instanceof MultimapViewFn
                      || viewFn instanceof MultimapViewFn2) {
                    @SuppressWarnings("unchecked")
                    ViewT viewT = (ViewT) getMultimapForWindow(tag, window);
                    return viewT;
                  } else if (viewFn
                      instanceof DataflowPortabilityPCollectionView.PortabilityViewFn) {
                    @SuppressWarnings("unchecked")
                    ViewT viewT = (ViewT) getPortabilityMultimapForWindow(tag, window);
                    return viewT;
                  }
                  throw new IllegalArgumentException("Unknown type of view requested: " + view);
                });
      }
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          String.format("Failed to materialize view %s for window %s.", view, window),
          e.getCause());
    }
  }

  /**
   * Returns the singleton for the provided window if available. Otherwise returns the default
   * specified within the view. This function expects at most one Ism file to contain a given window
   * and expects the Ism records to have been written out as:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Value: WindowedValue
   * </ul>
   */
  private <T, W extends BoundedWindow> T getSingletonForWindow(
      TupleTag<?> viewTag, HasDefaultValue<T> viewFn, W window) throws IOException {
    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "unchecked"
    })
    List<IsmReader<WindowedValue<T>>> readers = (List) tagToIsmReaderMap.get(viewTag);
    List<IsmReader<WindowedValue<T>>.IsmPrefixReaderIterator> readerIterators =
        findAndStartReaders(readers, ImmutableList.of(window));

    // If no reader iterator contains the value, return the default value stored
    // within the view.
    if (readerIterators.isEmpty()) {
      return viewFn.getDefaultValue();
    }

    checkState(
        readerIterators.size() == 1,
        "Expected only one Ism file to contain the singleton record for window %s",
        window);

    return readerIterators.get(0).getCurrent().getValue().getValue().getValue();
  }

  /** Get a map written as a singleton view due to a nondeterminstic key {@link Coder}. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  private <T, W extends BoundedWindow> T getMapSingletonForViewAndWindow(
      TupleTag<?> viewTag, W window) throws IOException {
    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "unchecked"
    })
    List<IsmReader<WindowedValue<T>>> readers = (List) tagToIsmReaderMap.get(viewTag);
    List<IsmReader<WindowedValue<T>>.IsmPrefixReaderIterator> readerIterators =
        findAndStartReaders(readers, ImmutableList.of(window));

    // If no reader iterator contains the value, return the default value stored
    // within the view.
    if (readerIterators.isEmpty()) {
      return (T) ImmutableMap.of();
    }

    checkState(
        readerIterators.size() == 1,
        "Expected only one Ism file to contain the singleton record for window %s",
        window);

    return readerIterators.get(0).getCurrent().getValue().getValue().getValue();
  }

  /**
   * Returns the list for the provided window if available. Otherwise returns an empty list. For the
   * non-global window, this function expects at most one Ism file to contain a given window and
   * expects the Ism records to have been written out as:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Key 2: Index offset within window
   *   <li>Value: WindowedValue
   * </ul>
   *
   * <p>For the global window, this expects that each file contains a segment of the list and that
   * the Ism records have been written out as:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Key 2: Index offset within file
   *   <li>Value: WindowedValue
   * </ul>
   *
   * The global offset is computed by summing the number of elements in each file.
   */
  private <T, W extends BoundedWindow> List<T> getListForWindow(TupleTag<?> tag, W window)
      throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<IsmReader<WindowedValue<T>>> readers = (List) tagToIsmReaderMap.get(tag);
    List<IsmReader<WindowedValue<T>>.IsmPrefixReaderIterator> readerIterators =
        findAndStartReaders(readers, ImmutableList.of(window));
    if (readerIterators.isEmpty()) {
      return Collections.emptyList();
    }
    checkState(
        GlobalWindow.INSTANCE.equals(window) || readerIterators.size() == 1,
        "Expected to have had PCollectionView %s for window %s contained within one file.",
        tag,
        window);
    return Collections.unmodifiableList(
        new ListOverReaderIterators<>(
            readerIterators, (WindowedValue<T> value) -> value.getValue()));
  }

  /**
   * Returns a map over a set of readers or an empty map if the window is not within the side input.
   * The {@code Ism} files are expected to be sharded by the hash of the key's byte representation.
   * Each record is required to be structured as follows:
   *
   * <ul>
   *   <li>Key 1: User key K
   *   <li>Key 2: Window
   *   <li>Key 3: 0L (constant)
   *   <li>Value: Windowed value
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   *
   * <ul>
   *   <li>Key 1: Metadata Key
   *   <li>Key 2: Window
   *   <li>Key 3: Index [0, size of map]
   *   <li>Value: variable length long byte representation of size of map if index is 0, otherwise
   *       the byte representation of a key
   * </ul>
   *
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while {@code
   * [META, Window, i]} for {@code i} in {@code [1, size of map]} stores the users key. This allows
   * for one to access the size of the map by looking at {@code [META, Window, 0]} and iterate over
   * all the keys by accessing {@code [META, Window, i]} for {@code i} in {@code [1, size of map]}.
   */
  private <K, V, W extends BoundedWindow> Map<K, V> getMapForWindow(TupleTag<?> tag, W window)
      throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<IsmReader<WindowedValue<V>>> readers = (List) tagToIsmReaderMap.get(tag);
    if (readers.isEmpty()) {
      return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    Coder<K> keyCoder =
        ((MetadataKeyCoder<K>) readers.get(0).getCoder().getKeyComponentCoder(0)).getKeyCoder();

    Long size =
        findMetadata(
            readers, ImmutableList.of(IsmFormat.getMetadataKey(), window, 0L), VarLongCoder.of());

    // If there is no metadata record for the window, we can return an empty map.
    if (size == null) {
      return Collections.emptyMap();
    }

    return Collections.unmodifiableMap(
        new MapOverReaders<K, V, V, W>(window, new MapToValue<K, V>(), readers, keyCoder, size));
  }

  /**
   * Returns a map over a set of readers or an empty map if the window is not within the side input.
   * The {@code Ism} files are expected to be sharded by the hash of the key's byte representation.
   * Each record is required to be structured as follows:
   *
   * <ul>
   *   <li>Key 1: User key K
   *   <li>Key 2: Window
   *   <li>Key 3: Index offset for a given key and window.
   *   <li>Value: Windowed value
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   *
   * <ul>
   *   <li>Key 1: Metadata Key
   *   <li>Key 2: Window
   *   <li>Key 3: Index [0, size of map]
   *   <li>Value: variable length long byte representation of size of map if index is 0, otherwise
   *       the byte representation of a key
   * </ul>
   *
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while {@code
   * [META, Window, i]} for {@code i} in {@code [1, size of map]} stores the users key. This allows
   * for one to access the size of the map by looking at {@code [META, Window, 0]} and iterate over
   * all the keys by accessing {@code [META, Window, i]} for {@code i} in {@code [1, size of map]}.
   */
  private <K, V, W extends BoundedWindow> Map<K, Iterable<V>> getMultimapForWindow(
      TupleTag<?> tag, W window) throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<IsmReader<WindowedValue<V>>> readers = (List) tagToIsmReaderMap.get(tag);
    if (readers.isEmpty()) {
      return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    Coder<K> keyCoder =
        ((MetadataKeyCoder<K>) readers.get(0).getCoder().getKeyComponentCoder(0)).getKeyCoder();

    Long size =
        findMetadata(
            readers, ImmutableList.of(IsmFormat.getMetadataKey(), window, 0L), VarLongCoder.of());

    // If there is no metadata record for the window, we can return an empty map.
    if (size == null) {
      return Collections.emptyMap();
    }

    return Collections.unmodifiableMap(
        new MapOverReaders<K, V, Iterable<V>, W>(
            window, new MapToIterable<K, V>(), readers, keyCoder, size));
  }

  /**
   * Returns a {@link MultimapView} over a set of readers. The {@code Ism} files are expected to be
   * sharded by the hash of the key's byte representation. Each record is required to be structured
   * as follows:
   *
   * <ul>
   *   <li>Key 1: User key K
   *   <li>Key 2: Window
   *   <li>Key 3: Index offset for a given key and window.
   *   <li>Value: Windowed value
   * </ul>
   *
   * <p>There are no metadata records associated with any records.
   */
  private <K, V, W extends BoundedWindow> MultimapView<K, V> getPortabilityMultimapForWindow(
      TupleTag<?> tag, W window) throws IOException {
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<IsmReader<V>> readers = (List) tagToIsmReaderMap.get(tag);

    if (readers.isEmpty()) {
      return InMemoryMultimapSideInputView.empty();
    }

    return new IsmMultimapView<>(window, readers);
  }

  /**
   * A {@link MultimapView} that fronts a set of readers. This view assumes that the readers have
   * records using the user key followed by window.
   */
  private class IsmMultimapView<K, V> implements MultimapView<K, V> {

    private final BoundedWindow window;
    private final List<IsmReader<V>> readers;

    private IsmMultimapView(BoundedWindow window, List<IsmReader<V>> readers) {
      this.window = window;
      this.readers = readers;
    }

    @Override
    public Iterable<K> get() {
      throw new UnsupportedOperationException("TODO: Support enumerating the keys.");
    }

    @Override
    public Iterable<V> get(K k) {
      k = checkArgumentNotNull(k);
      try {
        return new ListOverReaderIterators<>(
            findAndStartReaders(readers, ImmutableList.of(k, window)), (V value) -> value);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Unable to create view for window %s and key %s.", window, k), e);
      }
    }
  }

  /**
   * Returns the last key from each reader iterator. This function assumes that the last key
   * component is a long.
   */
  private <V> Collection<Long> getListIndexFromReaderIterators(
      List<IsmReader<V>.IsmPrefixReaderIterator> readerIterators) throws IOException {
    List<Callable<Long>> callables = new ArrayList<>();

    // Build a list of callables that will return the last key component assuming its a long
    for (final IsmReader<V>.IsmPrefixReaderIterator readerIterator : readerIterators) {
      callables.add(
          () -> {
            WindowedValue<IsmRecord<V>> last = readerIterator.getLast();
            if (last == null) {
              return 0L;
            }
            return ((long)
                    last.getValue().getKeyComponent(last.getValue().getKeyComponents().size() - 1))
                + 1L;
          });
    }

    try {
      List<Future<Long>> results = executorService.invokeAll(callables);
      List<Long> lastKeyComponents = new ArrayList<>(results.size());
      for (Future<Long> result : results) {
        lastKeyComponents.add(result.get());
      }
      return lastKeyComponents;
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      // Attempt to propagate the cause if possible.
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e);
    }
  }

  /**
   * Returns a list of reader iterators over the provided key components. Each reader iterator
   * within the returned list is guaranteed to have at least one element and will be in a state
   * where {@link NativeReader.NativeReaderIterator#start} has already been called.
   */
  private <V> List<IsmReader<V>.IsmPrefixReaderIterator> findAndStartReaders(
      List<IsmReader<V>> readers, final List<?> keyComponents) throws IOException {
    if (readers.isEmpty()) {
      return Collections.emptyList();
    }

    RandomAccessData keyBytes = new RandomAccessData();
    int shardId = readers.get(0).getCoder().encodeAndHash(keyComponents, keyBytes);
    List<IsmReader<V>.IsmPrefixReaderIterator> readerIterators = new ArrayList<>();
    for (final IsmReader<V> reader : readers) {
      IsmReader<V>.IsmPrefixReaderIterator readerIterator =
          reader.overKeyComponents(keyComponents, shardId, keyBytes);
      if (readerIterator.start()) {
        readerIterators.add(readerIterator);
      }
    }
    return readerIterators;
  }

  /**
   * Finds the metadata associated with the specific key components. Returns null if the metadata
   * does not exist.
   */
  private <V, T> T findMetadata(
      List<IsmReader<WindowedValue<V>>> readers, List<?> keyComponents, Coder<T> metadataCoder)
      throws IOException {

    // Find a set of reader iterators that have the requested key components.
    List<IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator> readerIterators =
        findAndStartReaders(readers, keyComponents);

    if (readerIterators.isEmpty()) {
      return null;
    }

    // We expect at most one such reader iterator to have been returned.
    IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator readerIterator =
        Iterables.getOnlyElement(readerIterators);

    // Decode the metadata
    return CoderUtils.decodeFromByteArray(
        metadataCoder, readerIterator.getCurrent().getValue().getMetadata());
  }

  /**
   * A list that fronts a set of reader iterators. This list assumes that that reader iterators are
   * already configured for the proper prefix. For an iterable/list view, this would be the window.
   * For a multimap, this would be the user key and window.
   */
  private class ListOverReaderIterators<T, V> extends AbstractList<V> {
    private final List<IsmReader<T>.IsmPrefixReaderIterator> readerIterators;
    private final ArrayList<Long> numberOfRecordsPerReaderIterator;
    private final Function<T, V> valueTypeToOutputType;

    private ListOverReaderIterators(
        List<IsmReader<T>.IsmPrefixReaderIterator> readerIterators,
        Function<T, V> valueTypeToOutputType)
        throws IOException {
      this.readerIterators = readerIterators;
      this.numberOfRecordsPerReaderIterator =
          new ArrayList<>(getListIndexFromReaderIterators(readerIterators));
      this.valueTypeToOutputType = valueTypeToOutputType;
    }

    // TODO: Support greater than Integer.MAX_VALUE values for iteration/lookup and size.
    @Override
    public V get(int index) {
      return getUsingLong(index);
    }

    @Override
    public int size() {
      return Ints.checkedCast(longSize());
    }

    @Override
    public Iterator<V> iterator() {
      return listIterator();
    }

    @Override
    public ListIterator<V> listIterator() {
      return new ListIteratorOverReaderIterators();
    }

    /** Returns the value at the specified position. */
    private V getUsingLong(long index) {
      try {
        if (index < 0) {
          throw new IndexOutOfBoundsException("Index out of range: " + index);
        }

        // We locate which reader iterator contains the requested index
        // by using the number of records contained within each reader iterator
        // as a way to compute the local offset. Once we find a local offset
        // which is within the bounds of the reader iterator, we use that local
        // offset to find the requested record.
        long localOffset = index;
        int readerIteratorIndex;
        for (readerIteratorIndex = 0;
            readerIteratorIndex < numberOfRecordsPerReaderIterator.size();
            ++readerIteratorIndex) {

          if (localOffset < numberOfRecordsPerReaderIterator.get(readerIteratorIndex)) {
            WindowedValue<IsmRecord<T>> rval =
                readerIterators.get(readerIteratorIndex).get(ImmutableList.of(localOffset));

            checkState(
                rval != null,
                "Expected to have found index %s, local offset %s within file.",
                index,
                localOffset);

            return valueTypeToOutputType.apply(rval.getValue().getValue());
          }
          localOffset -= numberOfRecordsPerReaderIterator.get(readerIteratorIndex);
        }

        // If we went past the last file then we seeked past the end and are out of bounds.
        throw new IndexOutOfBoundsException("Index out of range: " + index);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    private long longSize() {
      long total = 0;
      for (Long numRecords : numberOfRecordsPerReaderIterator) {
        total += numRecords;
      }
      return total;
    }

    /** An immutable list iterator that uses a long as its position. */
    private class ListIteratorOverReaderIterators implements ListIterator<V> {
      private long position;

      @Override
      public boolean hasNext() {
        return position < longSize();
      }

      @Override
      public V next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        V rval = getUsingLong(position);
        position += 1;
        return rval;
      }

      @Override
      public boolean hasPrevious() {
        return position > 0;
      }

      @Override
      public V previous() {
        if (!hasPrevious()) {
          throw new NoSuchElementException();
        }
        position -= 1;
        return getUsingLong(position);
      }

      @Override
      public int nextIndex() {
        return Ints.checkedCast(position);
      }

      @Override
      public int previousIndex() {
        return Ints.checkedCast(position - 1);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void set(V e) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void add(V e) {
        throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * A map that fronts a set of readers. This map assumes that the readers have records using the
   * user key followed by window.
   */
  private class MapOverReaders<K, V1, V2, W extends BoundedWindow> extends AbstractMap<K, V2> {
    private final W window;
    private final Function<KV<K, IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator>, V2>
        transform;
    private final List<IsmReader<WindowedValue<V1>>> readers;
    private final Coder<K> keyCoder;
    private final long size;

    private MapOverReaders(
        W window,
        Function<KV<K, IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator>, V2> transform,
        List<IsmReader<WindowedValue<V1>>> readers,
        Coder<K> keyCoder,
        long size) {

      this.window = window;
      this.transform = transform;
      this.readers = readers;
      this.keyCoder = keyCoder;
      this.size = size;
    }

    @Override
    public boolean containsKey(Object key) {
      try {
        return !findAndStartReaders(readers, ImmutableList.of(key, window)).isEmpty();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public V2 get(Object objectKey) {
      @SuppressWarnings("unchecked")
      K key = (K) objectKey;
      try {
        // We find the reader iterator which contains the key/window prefix.
        // For maps, this yields only one record. For multimaps, this is a valid
        // prefix reader iterator.
        List<IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator> readerIterators =
            findAndStartReaders(readers, ImmutableList.of(key, window));
        if (readerIterators.isEmpty()) {
          return null;
        }

        // Only one such reader iterator is expected.
        IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator readerIterator =
            Iterables.getOnlyElement(readerIterators);

        return transform.apply(KV.of(key, readerIterator));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public int size() {
      return Ints.checkedCast(size);
    }

    @Override
    public Set<Entry<K, V2>> entrySet() {
      return new EntrySet();
    }

    /** An entry set that is fronted by this map. */
    private class EntrySet extends AbstractSet<Entry<K, V2>> {
      @Override
      public boolean contains(Object o) {
        if (!(o instanceof Entry)) {
          return false;
        }
        @SuppressWarnings("unchecked")
        Entry<K, ?> entry = (Entry<K, ?>) o;
        try {
          // We find the reader iterator which contains the key/window prefix.
          // For maps, this yields only one record. For multimaps, this is a valid
          // prefix reader iterator.
          List<IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator> readerIterators =
              findAndStartReaders(readers, ImmutableList.of(entry.getKey(), window));
          if (readerIterators.isEmpty()) {
            return false;
          }

          // Only one such reader iterator is expected.
          IsmReader<WindowedValue<V1>>.IsmPrefixReaderIterator readerIterator =
              Iterables.getOnlyElement(readerIterators);

          return Objects.equal(
              entry.getValue(), transform.apply(KV.of(entry.getKey(), readerIterator)));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }

      @Override
      public Iterator<Entry<K, V2>> iterator() {
        return new EntrySetIterator();
      }

      @Override
      public int size() {
        return Ints.checkedCast(size);
      }
    }

    /**
     * An entry set iterator that backs this map which utilizes the [META, Window, Index] records to
     * locate subsequent keys.
     */
    private class EntrySetIterator implements Iterator<Entry<K, V2>> {
      long position = 0;

      @Override
      public boolean hasNext() {
        return position < size;
      }

      @Override
      public Entry<K, V2> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final K key;
        final V2 value;
        try {
          key =
              findMetadata(
                  readers,
                  ImmutableList.of(IsmFormat.getMetadataKey(), window, position + 1),
                  keyCoder);
          value = get(key);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        // Once we have fetched the key and value we can increment the position knowing that
        // an exception won't be thrown, thus allowing retries.
        position += 1;
        return new StructuralMapEntry<>(keyCoder, key, value);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * A function which is able to unwrap windowed values to just their values using a reader iterator
   * as input.
   */
  private static class MapToValue<K, V>
      implements Function<KV<K, IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator>, V> {

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public V apply(@Nonnull KV<K, IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator> input) {
      IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator startedReader = input.getValue();
      WindowedValue<IsmRecord<WindowedValue<V>>> value = startedReader.getCurrent();
      return value.getValue().getValue().getValue();
    }
  }

  /**
   * A function which is able to create a list over reader iterators using the reader iterator to
   * back it.
   */
  private class MapToIterable<K, V>
      implements Function<KV<K, IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator>, Iterable<V>> {

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Iterable<V> apply(
        @Nonnull KV<K, IsmReader<WindowedValue<V>>.IsmPrefixReaderIterator> input) {
      try {
        return Iterables.unmodifiableIterable(
            new ListOverReaderIterators<>(
                ImmutableList.of(input.getValue()), (WindowedValue<V> value) -> value.getValue()));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** A map entry which utilizes the structural value of the key for comparison. */
  private static class StructuralMapEntry<K, V> extends AbstractMap.SimpleImmutableEntry<K, V> {
    private final Coder<K> keyCoder;

    public StructuralMapEntry(Coder<K> keyCoder, K key, V value) {
      super(key, value);
      checkNotNull(keyCoder);
      this.keyCoder = keyCoder;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      try {
        @SuppressWarnings("unchecked")
        Map.Entry<K, V> other = (Map.Entry<K, V>) o;
        return Objects.equal(
                keyCoder.structuralValue(getKey()), keyCoder.structuralValue(other.getKey()))
            && Objects.equal(getValue(), other.getValue());
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public int hashCode() {
      try {
        return Objects.hashCode(keyCoder.structuralValue(getKey()), getValue());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(StructuralMapEntry.class)
          .add("key", getKey())
          .add("value", getValue())
          .add("keyCoder", keyCoder)
          .toString();
    }
  }
}
