/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.WindowedValue.valueInEmptyWindows;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.Footer;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.FooterCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecord;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecordCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmShard;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefix;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefixCoder;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.RandomAccessData;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter.ScalableBloomFilterCoder;
import com.google.cloud.dataflow.sdk.util.VarInt;
import com.google.cloud.dataflow.sdk.util.WeightedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@link NativeReader} that reads Ism files.
 *
 * @param <V> the type of the value written to the sink
 */
public class IsmReader<V> extends NativeReader<WindowedValue<IsmRecord<V>>> {
  /**
   * This constant represents the distance we would rather read and drop bytes for
   * versus doing an actual repositioning of the underlying stream. Tuned for operation
   * within GCS.
   */
  private static final int SEEK_VS_READ = 6 * 1024 * 1024;
  private static final int MAX_SHARD_INDEX_AND_FOOTER_SIZE = 1024 * 1024;

  private final String filename;
  private final IsmRecordCoder<V> coder;

  /** Lazily initialized on first read. */
  private long length;
  private Footer footer;

  /** A map indexed by shard id, storing the Ism shard descriptors. */
  private NavigableMap<Integer, IsmShard> shardIdToShardMap;

  /**
   * A map sorted and indexed by the block offset for a given shard. The sorting is important so
   * the {@link LazyIsmPrefixReaderIterator} can read through the file in increasing order.
   */
  private NavigableMap<Long, IsmShard> shardOffsetToShardMap;

  /** Values lazily initialized per shard on first keyed read of each shard. */
  private Map<Integer, ImmutableSortedMap<RandomAccessData, IsmShardKey>> indexPerShard;
  ScalableBloomFilter bloomFilter;

  /**
   * A cache instance which if set on this reader is used to cache blocks of data that are read.
   * Each value represents the decoded form of a block.
   */
  private final Cache<IsmShardKey,
                      WeightedValue<NavigableMap<RandomAccessData,
                                                 WindowedValue<IsmRecord<V>>>>> cache;

  /**
   * Produces a reader for the specified {@code filename} and {@code coder}.
   * See {@link IsmFormat} for encoded format details.
   */
  IsmReader(
      final String filename,
      IsmRecordCoder<V> coder,
      Cache<IsmShardKey, WeightedValue<NavigableMap<RandomAccessData,
                                                    WindowedValue<IsmRecord<V>>>>> cache) {
    checkNotNull(cache);
    IsmFormat.validateCoderIsCompatible(coder);
    this.filename = filename;
    this.coder = coder;
    this.cache = cache;
  }

  @Override
  public IsmPrefixReaderIterator iterator() throws IOException {
    return new LazyIsmPrefixReaderIterator();
  }

  /**
   * Returns a reader over a set of key components. The key components are encoded to their
   * byte representations and used as a key prefix.
   *
   * <p>If the file is empty or their is no key with the same prefix, then we return
   * an empty reader iterator.
   *
   * <p>If less than the required number of shard key components is passed in, then a reader
   * iterator over all the keys is returned.
   *
   * <p>Otherwise we return a reader iterator which only iterates over keys which have the
   * same key prefix.
   */
  public IsmPrefixReaderIterator overKeyComponents(List<?> keyComponents) throws IOException {
    checkNotNull(keyComponents);
    checkArgument(keyComponents.size() <= coder.getKeyComponentCoders().size(),
        "Expected at most %s key component(s) but received %s.",
        coder.getKeyComponentCoders().size(), keyComponents);

    Optional<SeekableByteChannel> inChannel =
        initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent());

    // If this file is empty, we can return an empty iterator.
    if (footer.getNumberOfKeys() == 0) {
      return new EmptyIsmPrefixReaderIterator(keyComponents);
    }

    // If not enough key components to figure out which shard was requested, we return a reader
    // iterator over all the keys.
    if (keyComponents.size() < coder.getNumberOfShardKeyCoders(keyComponents)) {
      return new ShardAwareIsmPrefixReaderIterator(keyComponents, openIfNeeded(inChannel));
    }

    RandomAccessData keyBytes = new RandomAccessData();
    int shardId = coder.encodeAndHash(keyComponents, keyBytes);

    // If this file does not contain the shard or Bloom filter does not contain the key prefix,
    // we know that we can return an empty reader iterator.
    if (!shardIdToShardMap.containsKey(shardId)) {
      return new EmptyIsmPrefixReaderIterator(keyComponents);
    }
    inChannel = initializeForKeyedRead(shardId, inChannel);
    closeIfPresent(inChannel);
    if (!bloomFilterMightContain(keyBytes)) {
      return new EmptyIsmPrefixReaderIterator(keyComponents);
    }

    // Otherwise we may actually contain the key so construct a reader iterator
    // which will fetch the data blocks containing the requested key prefix.

    // We find the first key in the index which may contain our prefix
    RandomAccessData floorKey = indexPerShard.get(shardId).floorKey(keyBytes);

    // We compute an upper bound on the key prefix by incrementing the prefix
    RandomAccessData keyBytesUpperBound = keyBytes.increment();
    // Compute the sub-range of the index map that we want to iterate over since
    // any of these blocks may contain the key prefix.
    Iterator<IsmShardKey> blockEntries =
        indexPerShard.get(shardId).subMap(floorKey, keyBytesUpperBound).values().iterator();

    return new WithinShardIsmPrefixReaderIterator(
        keyComponents,
        keyBytes,
        keyBytesUpperBound,
        blockEntries);
  }

  /** Returns the coder associated with this IsmReader. */
  public IsmRecordCoder<V> getCoder() {
    return coder;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(IsmReader.class)
        .add("filename", filename)
        .add("coder", coder)
        .toString();
  }

  // Overridable by tests to get around the bloom filter not containing any values.
  @VisibleForTesting
  boolean bloomFilterMightContain(RandomAccessData keyBytes) {
    return bloomFilter.mightContain(keyBytes.array(), 0, keyBytes.size());
  }

  /**
   * Initialize this Ism reader by reading the footer and shard index. Returns a channel for re-use
   * if this method was required to open one.
   */
  private synchronized Optional<SeekableByteChannel>
      initializeFooterAndShardIndex(Optional<SeekableByteChannel> inChannel) throws IOException {
    if (footer != null) {
      checkState(shardIdToShardMap != null,
          "Expected shard id to shard map to have been initialized.");
      checkState(shardOffsetToShardMap != null,
          "Expected shard offset to shard map to have been initialized.");
      return inChannel;
    }
    checkState(shardIdToShardMap == null,
        "Expected shard id to shard map to not have been initialized.");
    checkState(shardOffsetToShardMap == null,
        "Expected shard offset to shard map to not have been initialized.");

    SeekableByteChannel rawChannel = openIfNeeded(inChannel);
    this.length = rawChannel.size();

    // We read the last chunk of data, for small files we will capture the entire file.
    // We may capture the Bloom filter, shard index, and footer for slightly larger files.
    // Otherwise we are guaranteed to capture the footer and the shard index.
    long startPosition = Math.max(length - MAX_SHARD_INDEX_AND_FOOTER_SIZE, 0);
    position(rawChannel, startPosition);
    RandomAccessData data =
        new RandomAccessData(ByteStreams.toByteArray(Channels.newInputStream(rawChannel)));

    // Read the fixed length footer.
    this.footer = FooterCoder.of().decode(
        data.asInputStream(data.size() - Footer.FIXED_LENGTH, Footer.FIXED_LENGTH), Context.OUTER);

    checkState(startPosition < footer.getIndexPosition(),
        "Malformed file, expected to have been able to read entire shard index.");
    int offsetWithinReadData = (int) (footer.getIndexPosition() - startPosition);

    // Decode the list of Ism shard descriptors
    List<IsmShard> ismShards = IsmFormat.ISM_SHARD_INDEX_CODER.decode(
        data.asInputStream(offsetWithinReadData, data.size() - offsetWithinReadData),
        Context.NESTED);

    // Build the shard id to shard descriptor map
    ImmutableSortedMap.Builder<Integer, IsmShard> shardIdToShardMapBuilder =
        ImmutableSortedMap.orderedBy(Ordering.<Integer>natural());
    for (IsmShard ismShard : ismShards) {
      shardIdToShardMapBuilder.put(ismShard.getId(), ismShard);
    }
    shardIdToShardMap = shardIdToShardMapBuilder.build();

    // Build the shard block offset to shard descriptor map
    ImmutableSortedMap.Builder<Long, IsmShard> shardOffsetToShardMapBuilder =
        ImmutableSortedMap.orderedBy(Ordering.<Long>natural());
    for (IsmShard ismShard : ismShards) {
      shardOffsetToShardMapBuilder.put(ismShard.getBlockOffset(), ismShard);
    }
    shardOffsetToShardMap = shardOffsetToShardMapBuilder.build();

    // We may have gotten the Bloom filter, if so lets store it.
    if (startPosition < footer.getBloomFilterPosition()) {
      offsetWithinReadData = (int) (footer.getBloomFilterPosition() - startPosition);
      bloomFilter = ScalableBloomFilterCoder.of().decode(
          data.asInputStream(offsetWithinReadData, data.size() - offsetWithinReadData),
          Context.NESTED);
    }

    // TODO: We may have gotten the entire file so we should populate our cache to
    // prevent a re-read of the same data.

    return Optional.of(rawChannel);
  }

  /**
   * Initializes the Bloom filter and index per shard. We prepopulate empty indices
   * for shards where the index offset matches the following shard block offset.
   * Re-uses the provided channel, returning it or a new one if this method
   * was required to open one.
   */
  private synchronized Optional<SeekableByteChannel> initializeBloomFilterAndIndexPerShard(
      Optional<SeekableByteChannel> inChannel) throws IOException {
    if (indexPerShard != null) {
      checkState(bloomFilter != null, "Expected Bloom filter to have been initialized.");
      return inChannel;
    }

    SeekableByteChannel rawChannel = openIfNeeded(inChannel);

    // initializeFooterAndShardIndex may have initialized the Bloom filter
    // if the file is small enough.
    if (bloomFilter == null) {
      // Set the position to where the bloom filter is and read it in.
      position(rawChannel, footer.getBloomFilterPosition());
      bloomFilter = ScalableBloomFilterCoder.of().decode(
          Channels.newInputStream(rawChannel), Context.NESTED);
    }

    indexPerShard = new HashMap<>();
    // If a shard is small, it may not contain an index and we can detect this and
    // prepopulate the shard index map with an empty entry if the start of the index
    // and start of the next block are equal
    Iterator<IsmShard> shardIterator = shardOffsetToShardMap.values().iterator();

    // If file is empty we just return here.
    if (!shardIterator.hasNext()) {
      return Optional.of(rawChannel);
    }

    // If the current shard's index position is equal to the next shards block offset
    // then we know that the index contains no data and we can pre-populate it with
    // the empty map.
    IsmShard currentShard = shardIterator.next();
    while (shardIterator.hasNext()) {
      IsmShard nextShard = shardIterator.next();
      if (currentShard.getIndexOffset() == nextShard.getBlockOffset()) {
        indexPerShard.put(currentShard.getId(),
            ImmutableSortedMap.<RandomAccessData, IsmShardKey>orderedBy(
                RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR)
            .put(
                new RandomAccessData(0),
                new IsmShardKey(
                    IsmReader.this.filename,
                    new RandomAccessData(0),
                    currentShard.getBlockOffset(),
                    currentShard.getIndexOffset())).build());
      }
      currentShard = nextShard;
    }

    // Add an entry for the last shard if its index offset is equivalent to the
    // start of the Bloom filter, then we know that the index is empty.
    if (currentShard.getIndexOffset() == footer.getBloomFilterPosition()) {
      indexPerShard.put(currentShard.getId(),
          ImmutableSortedMap.<RandomAccessData, IsmShardKey>orderedBy(
              RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR)
          .put(
              new RandomAccessData(0),
              new IsmShardKey(
                  IsmReader.this.filename,
                  new RandomAccessData(0),
                  currentShard.getBlockOffset(),
                  currentShard.getIndexOffset())).build());
    }

    return Optional.of(rawChannel);
  }

  /** A unique key used to fully describe an Ism shard. */
  static class IsmShardKey {
    private final String filename;
    private final RandomAccessData firstKey;
    private final long startOffset;
    private final long endOffset;

    private IsmShardKey(
        String filename, RandomAccessData firstKey, long startOffset, long endOffset) {
      this.filename = filename;
      this.firstKey = firstKey;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(IsmShardKey.class)
          .add("filename", filename)
          .add("firstKey", firstKey)
          .add("startOffset", startOffset)
          .add("endOffset", endOffset)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IsmShardKey)) {
        return false;
      }
      IsmShardKey cacheEntry = (IsmShardKey) obj;
      return startOffset == cacheEntry.startOffset
          && endOffset == cacheEntry.endOffset
          && Objects.equals(filename, cacheEntry.filename)
          && Objects.equals(firstKey, cacheEntry.firstKey);
    }

    @Override
    public int hashCode() {
      return Longs.hashCode(startOffset) + Longs.hashCode(endOffset);
    }
  }

  /**
   * Initializes the footer, shard index, Bloom filter and index for the requested shard id if
   * they have not been initialized yet. Re-uses the provided channel, returning it or a
   * new one if this method was required to open one.
   */
  private Optional<SeekableByteChannel> initializeForKeyedRead(
      int shardId, Optional<SeekableByteChannel> inChannel) throws IOException {
    inChannel = initializeFooterAndShardIndex(inChannel);

    IsmShard shardWithIndex = shardIdToShardMap.get(shardId);

    // If this shard id is not within this file, we can return immediately.
    if (shardWithIndex == null) {
      return inChannel;
    }

    inChannel = initializeBloomFilterAndIndexPerShard(inChannel);

    // If the index has been populated and contains the shard id, we can return.
    if (indexPerShard != null && indexPerShard.containsKey(shardId)) {
      checkState(bloomFilter != null, "Bloom filter expected to have been initialized.");
      return inChannel;
    }

    checkState(indexPerShard.get(shardId) == null,
        "Expected to not have initialized index for shard %s", shardId);

    Long startOfNextBlock = shardOffsetToShardMap.higherKey(shardWithIndex.getBlockOffset());
    // If this is the last block, then we need to grab the position of the Bloom filter
    // as the upper bound.
    if (startOfNextBlock == null) {
      startOfNextBlock = footer.getBloomFilterPosition();
    }

    // Open the channel if needed and seek to the start of the index.
    SeekableByteChannel rawChannel = openIfNeeded(inChannel);
    rawChannel.position(shardWithIndex.getIndexOffset());
    InputStream inStream = Channels.newInputStream(rawChannel);

    ImmutableSortedMap.Builder<RandomAccessData, IsmShardKey> builder =
        ImmutableSortedMap.orderedBy(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR);

    // Read the first key
    RandomAccessData currentKeyBytes = new RandomAccessData();
    readKey(inStream, currentKeyBytes);
    long currentOffset = VarInt.decodeLong(inStream);

    // Insert the entry that happens at the beginning limiting the shard block by the
    // first keys block offset.
    builder.put(
        new RandomAccessData(0),
        new IsmShardKey(
            IsmReader.this.filename,
            new RandomAccessData(0),
            shardWithIndex.getBlockOffset(),
            currentOffset));

    // While another index entry exists, insert an index entry with the key, and offsets
    // that limit the range of the shard block.
    while (rawChannel.position() < startOfNextBlock) {
      RandomAccessData nextKeyBytes = currentKeyBytes.copy();
      readKey(inStream, nextKeyBytes);
      long nextOffset = VarInt.decodeLong(inStream);

      builder.put(currentKeyBytes,
          new IsmShardKey(
              IsmReader.this.filename, currentKeyBytes, currentOffset, nextOffset));

      currentKeyBytes = nextKeyBytes;
      currentOffset = nextOffset;
    }

    // Upper bound the last entry with the index offset.
    builder.put(currentKeyBytes,
        new IsmShardKey(
            IsmReader.this.filename,
            currentKeyBytes,
            currentOffset,
            shardWithIndex.getIndexOffset()));
    indexPerShard.put(shardId, builder.build());

    return Optional.of(rawChannel);
  }

  /** A function which takes an IsmShardKey fully describing a data block to read and return. */
  private class IsmCacheLoader
      implements Callable<WeightedValue<NavigableMap<RandomAccessData,
                                                     WindowedValue<IsmRecord<V>>>>> {

    private final IsmShardKey key;
    private IsmCacheLoader(IsmShardKey key) {
      this.key = key;
    }

    @Override
    public WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>> call()
          throws IOException {
      // Open a channel and build a sorted map from key to value for each key value
      // pair found within the data block.
      try (SeekableByteChannel rawChannel = open()) {
        try (WithinShardIsmReaderIterator readerIterator = new WithinShardIsmReaderIterator(
              rawChannel, key.firstKey, key.startOffset, key.endOffset)) {

          ImmutableSortedMap.Builder<RandomAccessData, WindowedValue<IsmRecord<V>>> mapBuilder =
              ImmutableSortedMap.orderedBy(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR);
          while (readerIterator.hasNext()) {
            RandomAccessData nextKey = readerIterator.peekKey().copy();
            WindowedValue<IsmRecord<V>> next = readerIterator.next();
            mapBuilder.put(nextKey, next);
          }
          // We return the size of the data block as the weight of this data block.
          return WeightedValue.of(
              (NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>) mapBuilder.build(),
              key.endOffset - key.startOffset);
        }
      }
    }
  }

  /**
   * Fetches the data block requested.
   *
   * If the cache is available, we will load and cache the requested block. Otherwise, we will
   * load and return the block.
   */
  private NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>
      fetch(IsmShardKey key) throws IOException {
    try {
      if (cache == null) {
        return new IsmCacheLoader(key).call().getValue();
      } else {
        return cache.get(key, new IsmCacheLoader(key)).getValue();
      }
    } catch (ExecutionException e) {
      // Try and re-throw the root cause if its an IOException
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e.getCause());
    }
  }

  /** The base class of Ism reader iterators which operate over a given key prefix. */
  abstract class IsmPrefixReaderIterator
      extends LegacyReaderIterator<WindowedValue<IsmRecord<V>>> {
    private final List<?> keyComponents;
    private IsmPrefixReaderIterator(List<?> keyComponents) {
      this.keyComponents = keyComponents;
    }

    /**
     * Returns the list of key components representing this iterators key prefix.
     */
    protected List<?> getKeyComponents() {
      return keyComponents;
    }

    /**
     * Concatenates this reader iterators key components with the additionally supplied
     * key components and encodes them into their byte representations producing a key.
     * Returns the exact record represented by the key generated above.
     *
     * Null is returned if no key has all the key components as a prefix within this file.
     */
    public final WindowedValue<IsmRecord<V>> get(List<?> additionalKeyComponents)
        throws IOException {
      RandomAccessData keyBytes = new RandomAccessData();
      int shardId = coder.encodeAndHash(
          ImmutableList.builder().addAll(keyComponents).addAll(additionalKeyComponents).build(),
          keyBytes);
      return getBlock(keyBytes, shardId).get(keyBytes);
    }

    /**
     * Returns the record for the last key having this iterators key prefix.
     * Last is defined as the largest key with the same key prefix when comparing key's
     * byte representations using an unsigned lexicographical byte order.
     *
     * Null is returned if the prefix is not present within this file.
     */
    public WindowedValue<IsmRecord<V>> getLast() throws IOException {
      RandomAccessData keyBytes = new RandomAccessData();
      int shardId = coder.encodeAndHash(keyComponents, keyBytes);

      Optional<SeekableByteChannel> inChannel =
          initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent());

      // Key is not stored here
      if (!shardIdToShardMap.containsKey(shardId)
          || !bloomFilterMightContain(keyBytes)) {
        return null;
      }

      inChannel = initializeForKeyedRead(shardId, inChannel);
      closeIfPresent(inChannel);

      final NavigableMap<RandomAccessData, IsmShardKey> indexInShard = indexPerShard.get(shardId);
      RandomAccessData end = keyBytes.increment();
      final IsmShardKey cacheEntry = indexInShard.floorEntry(end).getValue();

      NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> block = fetch(cacheEntry);

      RandomAccessData lastKey = block.lastKey();

      // If the requested key is greater then the last key within the block, then it
      // does not exist.
      if (RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(keyBytes, lastKey) > 0) {
        return null;
      }

      Entry<RandomAccessData, WindowedValue<IsmRecord<V>>> rval = block.floorEntry(end);

      // If the prefix matches completely then we can return
      if (RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.commonPrefixLength(
          keyBytes, rval.getKey()) == keyBytes.size()) {
        return rval.getValue();
      }
      return null;
    }
  }

  /** An empty reader iterator. */
  class EmptyIsmPrefixReaderIterator extends IsmPrefixReaderIterator {
    private EmptyIsmPrefixReaderIterator(List<?> keyComponents) {
      super(keyComponents);
    }

    @Override
    public boolean hasNextImpl() throws IOException {
      return false;
    }

    @Override
    public WindowedValue<IsmRecord<V>> nextImpl() throws IOException, NoSuchElementException {
      throw new NoSuchElementException();
    }
  }

  /** A reader iterator which initializes its input stream lazily. */
  class LazyIsmPrefixReaderIterator extends IsmPrefixReaderIterator {
    private IsmPrefixReaderIterator delegate;

    public LazyIsmPrefixReaderIterator() {
      super(ImmutableList.of());
    }

    @Override
    public boolean hasNextImpl() throws IOException {
      return getDelegate().hasNext();
    }

    @Override
    public WindowedValue<IsmRecord<V>> nextImpl()
        throws IOException, NoSuchElementException {
      WindowedValue<IsmRecord<V>> rval = getDelegate().next();
      return rval;
    }

    @Override
    public void close() throws IOException {
      if (delegate != null) {
        delegate.close();
      }
    }

    /** Return a reader, caching the creation on the first call. */
    private IsmPrefixReaderIterator getDelegate() throws IOException {
      if (delegate == null) {
        delegate = overKeyComponents(getKeyComponents());
      }
      return delegate;
    }
  }

  /**
   * Returns a map from key to value, where the keys are in increasing lexicographical order.
   * If the requested key is not contained within this file, an empty map is returned.
   */
  private NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>
      getBlock(RandomAccessData keyBytes, int shardId) throws IOException {
    Optional<SeekableByteChannel> inChannel =
        initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent());

    // Key is not stored here so return an empty map.
    if (!shardIdToShardMap.containsKey(shardId)
        || !bloomFilterMightContain(keyBytes)) {
      return ImmutableSortedMap.<RandomAccessData, WindowedValue<IsmRecord<V>>>orderedBy(
          RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR).build();
    }

    inChannel = initializeForKeyedRead(shardId, inChannel);
    closeIfPresent(inChannel);

    final NavigableMap<RandomAccessData, IsmShardKey> indexInShard = indexPerShard.get(shardId);
    final IsmShardKey cacheEntry = indexInShard.floorEntry(keyBytes).getValue();
    return fetch(cacheEntry);
  }

  /**
   * A reader iterator that returns all elements from prefix (inclusive) to
   * prefixUpperBound (exclusive) within the set of block entries provided.
   */
  private class WithinShardIsmPrefixReaderIterator extends IsmPrefixReaderIterator {
    private final Iterator<IsmShardKey> blockEntriesIterator;
    Iterator<WindowedValue<IsmRecord<V>>> iterator;
    private final RandomAccessData prefix;
    private final RandomAccessData prefixUpperBound;

    private WithinShardIsmPrefixReaderIterator(
        List<?> keyComponents,
        RandomAccessData prefix,
        RandomAccessData prefixUpperBound,
        Iterator<IsmShardKey> blockEntriesIterator) {
      super(keyComponents);
      checkNotNull(blockEntriesIterator);
      this.prefix = prefix;
      this.prefixUpperBound = prefixUpperBound;
      this.blockEntriesIterator = blockEntriesIterator;
    }

    @Override
    public boolean hasNextImpl() throws IOException {
      // This is in a while loop because the blocks that we are asked to look into may
      // not contain the key prefix.
      while (iterator == null || !iterator.hasNext()) {
        // If there are no blocks to iterate over we can return false
        if (!blockEntriesIterator.hasNext()) {
          return false;
        }

        IsmShardKey nextBlock = blockEntriesIterator.next();
        NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> map =
            fetch(nextBlock);
        SortedMap<RandomAccessData, WindowedValue<IsmRecord<V>>> submap =
            map.subMap(prefix, prefixUpperBound);
        Collection<WindowedValue<IsmRecord<V>>> values = submap.values();
        iterator = values.iterator();
      }
      return iterator.hasNext();
    }

    @Override
    public WindowedValue<IsmRecord<V>> nextImpl()
        throws IOException, NoSuchElementException {
      return iterator.next();
    }
  }

  /** A reader iterator that returns all records across all shards contained within this file. */
  private class ShardAwareIsmPrefixReaderIterator extends IsmPrefixReaderIterator {
    private final SeekableByteChannel rawChannel;
    private WithinShardIsmReaderIterator delegate;
    private Iterator<IsmShard> shardEntries;

    private ShardAwareIsmPrefixReaderIterator(
        List<?> keyComponents, SeekableByteChannel rawChannel) throws IOException {
      super(keyComponents);
      checkState(shardOffsetToShardMap.size() > 0,
          "Expected that shard offset to shard map has been initialized and is not empty.");

      this.rawChannel = rawChannel;
      this.shardEntries = shardOffsetToShardMap.values().iterator();
      IsmShard firstShard = shardEntries.next();
      delegate = new WithinShardIsmReaderIterator(
          rawChannel,
          new RandomAccessData(),
          firstShard.getBlockOffset(),
          firstShard.getIndexOffset());
    }

    @Override
    public boolean hasNextImpl() throws IOException {
      // If the current shard has a value, or we have another shard index to
      // look into then we know that there is another value.
      return delegate.hasNext() || shardEntries.hasNext();
    }

    @Override
    public WindowedValue<IsmRecord<V>> nextImpl()
        throws IOException, NoSuchElementException {
      // If our current shard index is empty, we need to move to the next one.
      if (!delegate.hasNext()) {
        moveToNextShard();
      }
      WindowedValue<IsmRecord<V>> rval = delegate.next();
      return rval;
    }

    @Override
    public void close() throws IOException {
      rawChannel.close();
    }

    @Override
    public WindowedValue<IsmRecord<V>> getLast() throws IOException {
      // Since this is an iterator over all of the file, we return the last record within the file.
      int lastShardId = shardOffsetToShardMap.lastEntry().getValue().getId();
      initializeForKeyedRead(lastShardId, Optional.<SeekableByteChannel>absent());
      NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> lastBlock =
          getBlock(indexPerShard.get(lastShardId).lastKey(), lastShardId);
      return lastBlock.lastEntry().getValue();
    }

    private void moveToNextShard() throws IOException {
      IsmShard nextIsmShard = shardEntries.next();
      checkState(nextIsmShard.getBlockOffset() >= rawChannel.position(),
          "Expected channel read order to be sequential.");

      delegate = new WithinShardIsmReaderIterator(
          rawChannel, new RandomAccessData(),
          nextIsmShard.getBlockOffset(), nextIsmShard.getIndexOffset());

      checkState(delegate.hasNext(), "Expected each shard to contain at least one entry.");
    }
  }

  /**
   * A reader iterator for Ism formatted files which returns a sequence of Ism records
   * from the underlying channel from where it is currently positioned till
   * the supplied limit.
   *
   * This reader iterator will use the supplied bytes as the bytes for the previous key
   * which is required to do the KeyPrefix based key decoding.
   */
  private class WithinShardIsmReaderIterator
      extends LegacyReaderIterator<WindowedValue<IsmRecord<V>>> {
    private final SeekableByteChannel rawChannel;
    private final InputStream inStream;
    private long readLimit;
    private RandomAccessData keyBytes;
    private boolean keyIsPeeked;
    private long position;

    private WithinShardIsmReaderIterator(
        SeekableByteChannel rawChannel,
        RandomAccessData currentKeyBytes,
        long newPosition,
        long newLimit) throws IOException {
      checkNotNull(rawChannel);
      checkNotNull(currentKeyBytes);
      checkArgument(newLimit >= 0L);
      checkArgument(newPosition <= newLimit);
      this.rawChannel = rawChannel;
      this.inStream = Channels.newInputStream(rawChannel);
      this.keyBytes = currentKeyBytes.copy();
      this.position = newPosition;
      this.readLimit = newLimit;
      this.keyIsPeeked = false;
      IsmReader.position(rawChannel, newPosition);
    }

    @Override
    public boolean hasNextImpl() throws IOException {
      if (position > readLimit) {
        throw new IllegalStateException("Read past end of stream");
      }
      return position < readLimit;
    }

    @Override
    public WindowedValue<IsmRecord<V>> nextImpl() throws IOException, NoSuchElementException {
      RandomAccessData peekedKey = peekKey();
      keyIsPeeked = false;

      InputStream keyInputStream = peekedKey.asInputStream(0, peekedKey.size());
      List<Object> keyComponents = new ArrayList<>(coder.getKeyComponentCoders().size());
      for (int i = 0; i < coder.getKeyComponentCoders().size(); ++i) {
        keyComponents.add(coder.getKeyComponentCoder(i).decode(
                keyInputStream,
                Context.NESTED));
      }

      final IsmRecord<V> ismRecord;
      if (IsmFormat.isMetadataKey(keyComponents)) {
        byte[] metadata = ByteArrayCoder.of().decode(inStream, Context.NESTED);
        ismRecord = IsmRecord.<V>meta(keyComponents, metadata);
      } else {
        V value = coder.getValueCoder().decode(inStream, Context.NESTED);
        ismRecord = IsmRecord.<V>of(keyComponents, value);
      }

      long newPosition = rawChannel.position();
      notifyElementRead(newPosition - position);
      position = newPosition;
      return valueInEmptyWindows(ismRecord);
    }

    public RandomAccessData peekKey() throws IOException, NoSuchElementException {
      if (keyIsPeeked) {
        return keyBytes;
      }
      readKey(inStream, keyBytes);
      keyIsPeeked = true;

      return keyBytes;
    }
  }

  /**
   * Decodes a KeyPrefix from the stream and then reads unshared key bytes from the stream
   * placing them into the supplied keyBytes at position
   * keyBytes[shared key bytes : shared key bytes + unshared key bytes].
   */
  private static void readKey(InputStream inStream, RandomAccessData keyBytes)
      throws IOException {
    KeyPrefix keyPrefix = KeyPrefixCoder.of().decode(inStream, Context.NESTED);
    // currentKey = prevKey[0 : sharedKeySize] + read(unsharedKeySize)
    keyBytes.readFrom(
        inStream,
        keyPrefix.getSharedKeySize() /* start to overwrite the previous key at sharedKeySize */,
        keyPrefix.getUnsharedKeySize() /* read unsharedKeySize bytes from the stream */);
    // Reset the length incase the next key was shorter.
    keyBytes.resetTo(keyPrefix.getSharedKeySize() + keyPrefix.getUnsharedKeySize());
  }

  /** Closes the underlying channel if present. */
  private void closeIfPresent(Optional<SeekableByteChannel> inChannel) throws IOException {
    if (inChannel.isPresent()) {
      inChannel.get().close();
    }
  }

  /** Returns a channel, opening a new one if required. */
  private SeekableByteChannel openIfNeeded(Optional<SeekableByteChannel> inChannel)
      throws IOException {
    if (inChannel.isPresent()) {
      return inChannel.get();
    }
    return open();
  }

  /** Opens a new channel. */
  private SeekableByteChannel open()
      throws IOException {
    ReadableByteChannel channel = IOChannelUtils.getFactory(filename).open(filename);
    Preconditions.checkArgument(channel instanceof SeekableByteChannel,
        "IsmReader requires a SeekableByteChannel for path %s but received %s.",
        filename, channel);
    return (SeekableByteChannel) channel;
  }

  /**
   * Seeks into the channel intelligently by either resetting the position or reading and
   * discarding bytes.
   */
  private static void position(SeekableByteChannel inChannel, long newPosition) throws IOException {
    long currentPosition = inChannel.position();
    // If just doing a read is cheaper discarding the bytes lets just do the read
    if (currentPosition < newPosition && newPosition - currentPosition <= SEEK_VS_READ) {
      ByteStreams.skipFully(Channels.newInputStream(inChannel), newPosition - currentPosition);
    } else {
      // Otherwise we will perform a seek
      inChannel.position(newPosition);
    }
  }
}
