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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable.Blocks;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams.DataStreamDecoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * Adapters which convert a a logical series of chunks using continuation tokens over the Beam Fn
 * State API into an {@link Iterator} of {@link ByteString}s.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class StateFetchingIterators {

  // do not instantiate
  private StateFetchingIterators() {}

  /**
   * This adapter handles using the continuation token to provide iteration over all the elements
   * returned by the Beam Fn State API using the supplied state client, state request for the first
   * chunk of the state stream, and a value decoder.
   *
   * <p>The cache's eviction policy will control how much if any the pages are stored in memory and
   * for how long mutations are stored.
   *
   * <p>Note: Mutation of the iterable only mutates the underlying cache. It is expected that
   * mutations will have been persisted to the runner such that future reads will reflect those
   * changes.
   *
   * @param cache A cache instance used to store pages of elements and any pending requests.
   * @param beamFnStateClient A client for handling state requests.
   * @param stateRequestForFirstChunk A fully populated state request for the first (and possibly
   *     only) chunk of a state stream. This state request will be populated with a continuation
   *     token to request further chunks of the stream if required.
   * @param valueCoder A coder for decoding the state stream.
   */
  public static <T> CachingStateIterable<T> readAllAndDecodeStartingFrom(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      StateRequest stateRequestForFirstChunk,
      Coder<T> valueCoder) {
    return new CachingStateIterable<>(
        (Cache<StateKey, Blocks<T>>) cache,
        beamFnStateClient,
        stateRequestForFirstChunk,
        valueCoder);
  }

  /** A mutable iterable that supports prefetch and is backed by a cache. */
  static class CachingStateIterable<T> implements PrefetchableIterable<T> {

    /** Represents a set of elements. */
    abstract static class Blocks<T> implements Weighted {
      abstract public List<Block<T>> getBlocks();
    }

    static class MutatedBlocks<T> extends Blocks<T> {
      private final Block<T> wholeBlock;
      MutatedBlocks(Block<T> wholeBlock) {
        this.wholeBlock = wholeBlock;
      }

      @Override
      public List<Block<T>> getBlocks() {
        return Collections.singletonList(wholeBlock);
      }

      @Override
      public long getWeight() {
        return wholeBlock.getWeight();
      }
    }

    /** Represents a logical prefix of elements for the logical stream over the state API. This prefix cannot represent a mutated in memory representation. */
    static class BlocksPrefix<T> extends Blocks<T> implements Shrinkable<Blocks<T>> {
      private final List<Block<T>> blocks;
      @Override
      public long getWeight() {
        try {
          long sum = 0;
          for (int i = 0; i < blocks.size(); ++i) {
            sum = Math.addExact(sum, blocks.get(i).getWeight());
          }
          return sum;
        } catch (ArithmeticException e) {
          return Long.MAX_VALUE;
        }
      }

      BlocksPrefix(List<Block<T>> blocks) {
        this.blocks = blocks;
      }

      @Override
      public Blocks<T> shrink() {
        // Copy the list to not hold a reference to the tail of the original list.
        List<Block<T>> subList = new ArrayList<>(getBlocks().subList(0, getBlocks().size() / 2));
        if (subList.isEmpty()) {
          return null;
        }
        return new BlocksPrefix<>(subList);
      }

      @Override
      public List<Block<T>> getBlocks() {
        return blocks;
      }
    }


    @AutoValue
    abstract static class Block<T> implements Weighted {
      public static <T> Block<T> mutatedBlock(List<T> values, long weight) {
        return new AutoValue_StateFetchingIterators_CachingStateIterable_Block<>(
            values, null, weight);
      }

      public static <T> Block<T> fromValues(List<T> values, @Nullable ByteString nextToken) {
        return new AutoValue_StateFetchingIterators_CachingStateIterable_Block<>(
            values, nextToken, Caches.weigh(values) + Caches.weigh(nextToken));
      }

      abstract List<T> getValues();

      abstract @Nullable ByteString getNextToken();

      @Override
      public abstract long getWeight();
    }

    private final Cache<StateKey, Blocks<T>> cache;
    private final BeamFnStateClient beamFnStateClient;
    private final StateRequest stateRequestForFirstChunk;
    private final Coder<T> valueCoder;

    public CachingStateIterable(
        Cache<StateKey, Blocks<T>> cache,
        BeamFnStateClient beamFnStateClient,
        StateRequest stateRequestForFirstChunk,
        Coder<T> valueCoder) {
      this.cache = cache;
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
      this.valueCoder = valueCoder;
    }

    /**
     * Clears the cached iterable and appends the set of values.
     *
     * <p>Mutations over the Beam Fn State API must have been performed before any future lookups.
     *
     * <p>Ensures that a cache entry exists for the entire iterable enabling future lookups to miss
     * requesting data from the state cache.
     */
    public void clearAndAppend(List<T> values) {
      cache.put(stateRequestForFirstChunk.getStateKey(), new MutatedBlocks<>(Block.mutatedBlock(new ArrayList<>(values), Caches.weigh(values))));
    }

    @Override
    public PrefetchableIterator<T> iterator() {
      return new CachingStateIterator();
    }

    /**
     * Appends the values to the cached iterable.
     *
     * <p>Mutations over the Beam Fn State API must have been performed before any future lookups.
     *
     * <p>A cache entry will only continue to exist if the entire iterable has been loaded into the
     * cache.
     *
     * <p>Takes ownership over the passed in list, the caller must not mutate it.
     */
    public void append(List<T> values) {
      Blocks<T> existing = cache.peek(stateRequestForFirstChunk.getStateKey());
      if (existing == null) {
        return;
      }
      // Check to see if we have cached the whole iterable, if not then we must remove it to prevent
      // returning invalid results as part of a future request.
      if (existing.getBlocks().get(existing.getBlocks().size() - 1).getNextToken() != null) {
        cache.remove(stateRequestForFirstChunk.getStateKey());
      }

      // Combine all the individual blocks into one block containing all the values since
      // they were mutated and we must evict all or none of the blocks. When consuming the blocks,
      // we must have a reference to all or none of the blocks (which fores a load).
      int totalSize = values.size();
      long totalWeight = Caches.weigh(values);
      List<Block<T>> chunks = existing.getBlocks();
      for (int i = 0; i < chunks.size(); ++i) {
        totalSize += chunks.get(i).getValues().size();
        try {
          totalWeight = Math.addExact(totalWeight, chunks.get(i).getWeight());
        } catch (ArithmeticException e) {
          totalWeight = Long.MAX_VALUE;
        }
      }
      List<T> allValues = new ArrayList<>(totalSize);
      for (int i = 0; i < chunks.size(); ++i) {
        allValues.addAll(chunks.get(i).getValues());
      }
      allValues.addAll(values);

      cache.put(stateRequestForFirstChunk.getStateKey(), new MutatedBlocks<>(Block.mutatedBlock(allValues, totalWeight)));
    }

    class CachingStateIterator implements PrefetchableIterator<T> {

      private final LazyBlockingStateFetchingIterator underlyingStateFetchingIterator;
      private final DataStreamDecoder<T> dataStreamDecoder;
      private Block<T> currentBlock;
      private int currentCachedBlockValueIndex;

      public CachingStateIterator() {
        this.underlyingStateFetchingIterator =
            new LazyBlockingStateFetchingIterator(
                Caches.subCache(cache, "StateFetch"), beamFnStateClient, stateRequestForFirstChunk);
        this.dataStreamDecoder =
            new DataStreamDecoder<>(valueCoder, underlyingStateFetchingIterator);
        this.currentBlock =
            Block.fromValues(
                Collections.emptyList(), stateRequestForFirstChunk.getGet().getContinuationToken());
        this.currentCachedBlockValueIndex = 0;
      }

      @Override
      public boolean isReady() {
        for (; ; ) {
          if (currentBlock.getValues().size() > currentCachedBlockValueIndex) {
            return true;
          }
          if (currentBlock.getNextToken() == null) {
            return true;
          }
          Blocks<T> existing = cache.peek(stateRequestForFirstChunk.getStateKey());
          boolean isFirstBlock = ByteString.EMPTY.equals(currentBlock.getNextToken());
          if (existing == null) {
            // If there is nothing cached and we are on the first block then we are not ready.
            return false;
          } else {
            if (isFirstBlock) {
              currentBlock = existing.getBlocks().get(0);
              currentCachedBlockValueIndex = 0;
            } else {
              List<Block<T>> blocks = existing.getBlocks();
              int currentBlockIndex = 0;
              for (; currentBlockIndex < blocks.size(); ++currentBlockIndex) {
                if (currentBlock.getNextToken().equals(blocks.get(currentBlockIndex).getNextToken())) {
                  break;
                }
              }
              // Move to the next block from cache if it was found.
              if (currentBlockIndex + 1 < blocks.size()) {
                currentBlock = blocks.get(currentBlockIndex + 1);
                currentCachedBlockValueIndex = 0;
              } else {
                // If not found, then we need to load it.
                return false;
              }
            }
          }
        }
      }

      @Override
      public void prefetch() {
        if (!isReady()) {
          underlyingStateFetchingIterator.seekToContinuationToken(currentBlock.getNextToken());
          dataStreamDecoder.consumeRemainderOfCurrentByteString();
          dataStreamDecoder.prefetch();
        }
      }

      @Override
      public boolean hasNext() {
        for (; ; ) {
          if (currentBlock.getValues().size() > currentCachedBlockValueIndex) {
            return true;
          }
          if (currentBlock.getNextToken() == null) {
            return true;
          }
          Blocks<T> existing = cache.peek(stateRequestForFirstChunk.getStateKey());
          boolean isFirstBlock = ByteString.EMPTY.equals(currentBlock.getNextToken());
          if (existing == null) {
            currentBlock = loadNextBlock(currentBlock.getNextToken());
            if (isFirstBlock) {
              cache.put(stateRequestForFirstChunk.getStateKey(), new BlocksPrefix<>(Collections.singletonList(currentBlock)));
            }
          } else {
            if (isFirstBlock) {
              currentBlock = existing.getBlocks().get(0);
            } else {
              checkState(!(existing instanceof BlocksPrefix), "Unexpected blocks type %s, expected a %s.", existing.getClass(), BlocksPrefix.class);
              List<Block<T>> blocks = existing.getBlocks();
              int currentBlockIndex = 0;
              for (; currentBlockIndex < blocks.size(); ++currentBlockIndex) {
                if (currentBlock.getNextToken().equals(blocks.get(currentBlockIndex).getNextToken())) {
                  break;
                }
              }
              // Load the next block from cache if it was found.
              if (currentBlockIndex + 1 < blocks.size()) {
                currentBlock = blocks.get(currentBlockIndex + 1);
              } else {
                // Otherwise load the block from state API.
                currentBlock = loadNextBlock(currentBlock.getNextToken());

                // Append this block to the existing set of blocks if it is logically the next one.
                if (currentBlockIndex == blocks.size() - 1) {
                  List<Block<T>> newBlocks = new ArrayList<>(currentBlockIndex + 1);
                  newBlocks.addAll(blocks);
                  newBlocks.add(currentBlock);
                  cache.put(stateRequestForFirstChunk.getStateKey(), new BlocksPrefix<>(newBlocks));
                }
              }
            }
          }
          currentCachedBlockValueIndex = 0;
        }
      }

      @VisibleForTesting
      Block<T> loadNextBlock(ByteString continuationToken) {
        underlyingStateFetchingIterator.seekToContinuationToken(continuationToken);
        dataStreamDecoder.seekToNextByteString();

        List<T> values = dataStreamDecoder.decodeTillAtChunkBoundary();
        ByteString nextToken = underlyingStateFetchingIterator.getContinuationToken();
        return Block.fromValues(values, nextToken);
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return currentBlock.getValues().get(currentCachedBlockValueIndex++);
      }
    }
  }

  /**
   * An {@link Iterator} which fetches {@link ByteString} chunks using the State API.
   *
   * <p>This iterator will only request a chunk on first access. Subsequently it eagerly pre-fetches
   * one future chunk at a time.
   */
  @VisibleForTesting
  static class LazyBlockingStateFetchingIterator implements PrefetchableIterator<ByteString> {

    private final Cache<ByteString, CompletableFuture<StateResponse>> cache;
    private final BeamFnStateClient beamFnStateClient;
    private final StateRequest stateRequestForFirstChunk;
    private ByteString continuationToken;
    private CompletableFuture<StateResponse> prefetchedResponse;

    LazyBlockingStateFetchingIterator(
        Cache<?, ?> cache,
        BeamFnStateClient beamFnStateClient,
        StateRequest stateRequestForFirstChunk) {
      this.cache = (Cache<ByteString, CompletableFuture<StateResponse>>) cache;
      this.beamFnStateClient = beamFnStateClient;
      this.stateRequestForFirstChunk = stateRequestForFirstChunk;
      this.continuationToken = stateRequestForFirstChunk.getGet().getContinuationToken();
    }

    /**
     * Returns the continuation token used to load the value returned by {@link #next}. Returns
     * {@code null} if there are no more values.
     */
    public @Nullable ByteString getContinuationToken() {
      return continuationToken;
    }

    /**
     * Repositions this {@link Iterator} such that the value returned by {@link #next} uses the
     * continuation token.
     *
     * <p>Passing in {@code null} seeks to the end of the stream.
     *
     * <p>This is a no-op if the continuation token passed in is the same as the current
     * continuation token.
     */
    public void seekToContinuationToken(@Nullable ByteString continuationToken) {
      // Don't clear the prefetched response if no seeking is required.
      if (Objects.equals(this.continuationToken, continuationToken)) {
        return;
      }
      this.continuationToken = continuationToken;
      this.prefetchedResponse = null;
    }

    @Override
    public boolean isReady() {
      if (prefetchedResponse == null) {
        return continuationToken == null;
      }
      return prefetchedResponse.isDone();
    }

    @Override
    public void prefetch() {
      if (continuationToken != null && prefetchedResponse == null) {
        prefetchedResponse = cache.computeIfAbsent(continuationToken, this::loadPrefetchedResponse);
      }
    }

    public CompletableFuture<StateResponse> loadPrefetchedResponse(ByteString continuationToken) {
      return beamFnStateClient.handle(
          stateRequestForFirstChunk
              .toBuilder()
              .setGet(StateGetRequest.newBuilder().setContinuationToken(continuationToken)));
    }

    @Override
    public boolean hasNext() {
      return continuationToken != null;
    }

    @Override
    public ByteString next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      prefetch();
      StateResponse stateResponse;
      try {
        stateResponse = prefetchedResponse.get();
        cache.remove(continuationToken);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        if (e.getCause() == null) {
          throw new IllegalStateException(e);
        }
        Throwables.throwIfUnchecked(e.getCause());
        throw new IllegalStateException(e.getCause());
      }
      prefetchedResponse = null;

      // If the continuation token is empty, that means we have reached EOF.
      if (ByteString.EMPTY.equals(stateResponse.getGet().getContinuationToken())) {
        continuationToken = null;
      } else {
        continuationToken = stateResponse.getGet().getContinuationToken();
        prefetch();
      }
      return stateResponse.getGet().getData();
    }
  }
}
