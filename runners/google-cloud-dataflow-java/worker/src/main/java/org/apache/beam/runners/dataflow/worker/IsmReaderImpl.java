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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
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
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.Footer;
import org.apache.beam.runners.dataflow.internal.IsmFormat.FooterCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmShard;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefix;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefixCoder;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.util.ScalableBloomFilter;
import org.apache.beam.runners.dataflow.worker.util.ScalableBloomFilter.ScalableBloomFilterCoder;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSortedMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link NativeReader} that reads Ism files.
 *
 * @param <V> the type of the value written to the sink
 */
// Possible real inconsistency - https://issues.apache.org/jira/browse/BEAM-6560
@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
public class IsmReaderImpl<V> extends IsmReader<V> {
  /**
   * This constant represents the distance we would rather read and drop bytes for versus doing an
   * actual repositioning of the underlying stream. Tuned for operation within GCS.
   */
  private static final int SEEK_VS_READ = 6 * 1024 * 1024;

  static final int MAX_SHARD_INDEX_AND_FOOTER_SIZE = 1024 * 1024;

  private final ResourceId resourceId;
  private final IsmRecordCoder<V> coder;

  /** Lazily initialized on first read. */
  private long length;

  private Footer footer;

  /** A map indexed by shard id, storing the Ism shard descriptors. */
  private NavigableMap<Integer, IsmShard> shardIdToShardMap;

  /**
   * A map sorted and indexed by the block offset for a given shard. The sorting is important so the
   * {@link LazyIsmPrefixReaderIterator} can read through the file in increasing order.
   */
  private NavigableMap<Long, IsmShard> shardOffsetToShardMap;

  /** Values lazily initialized per shard on first keyed read of each shard. */
  private Map<Integer, ImmutableSortedMap<RandomAccessData, IsmShardKey>> indexPerShard;

  ScalableBloomFilter bloomFilter;

  /**
   * A cache instance which if set on this reader is used to cache blocks of data that are read.
   * Each value represents the decoded form of a block.
   */
  private final Cache<
          IsmShardKey, WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>>>
      cache;

  /**
   * Produces a reader for the specified {@code resourceId} and {@code coder}. See {@link IsmFormat}
   * for encoded format details.
   */
  IsmReaderImpl(
      final ResourceId resourceId,
      IsmRecordCoder<V> coder,
      Cache<IsmShardKey, WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>>>
          cache) {
    checkNotNull(cache);
    IsmFormat.validateCoderIsCompatible(coder);
    this.resourceId = resourceId;
    this.coder = coder;
    this.cache = cache;
  }

  @Override
  public IsmPrefixReaderIterator iterator() throws IOException {
    SideInputReadCounter readCounter = IsmReader.getCurrentSideInputCounter();
    return new LazyIsmPrefixReaderIterator(readCounter);
  }

  @Override
  public IsmPrefixReaderIterator overKeyComponents(List<?> keyComponents) throws IOException {
    if (keyComponents.isEmpty()) {
      return overKeyComponents(keyComponents, 0, new RandomAccessData(0));
    }
    RandomAccessData keyBytes = new RandomAccessData();
    int shardId = coder.encodeAndHash(keyComponents, keyBytes);
    return overKeyComponents(keyComponents, shardId, keyBytes);
  }

  @Override
  public IsmPrefixReaderIterator overKeyComponents(
      List<?> keyComponents, int shardId, RandomAccessData keyBytes) throws IOException {
    checkNotNull(keyComponents);
    checkNotNull(keyBytes);
    SideInputReadCounter readCounter = IsmReader.getCurrentSideInputCounter();
    if (keyComponents.isEmpty()) {
      checkArgument(
          shardId == 0 && keyBytes.size() == 0,
          "Expected shard id to be 0 and key bytes to be empty "
              + "but got shard id %s and key bytes of length %s",
          shardId,
          keyBytes.size());
    }
    checkArgument(
        keyComponents.size() <= coder.getKeyComponentCoders().size(),
        "Expected at most %s key component(s) but received %s.",
        coder.getKeyComponentCoders().size(),
        keyComponents);

    Optional<SeekableByteChannel> inChannel =
        initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent(), readCounter);

    // If this file is empty, we can return an empty iterator.
    if (footer.getNumberOfKeys() == 0) {
      return new EmptyIsmPrefixReaderIterator(keyComponents);
    }

    // If not enough key components to figure out which shard was requested, we return a reader
    // iterator over all the keys.
    if (keyComponents.size() < coder.getNumberOfShardKeyCoders(keyComponents)) {
      return new ShardAwareIsmPrefixReaderIterator(
          keyComponents, openIfNeeded(inChannel), readCounter);
    }

    // If this file does not contain the shard or Bloom filter does not contain the key prefix,
    // we know that we can return an empty reader iterator.
    if (!shardIdToShardMap.containsKey(shardId)) {
      return new EmptyIsmPrefixReaderIterator(keyComponents);
    }
    inChannel = initializeForKeyedRead(shardId, inChannel, readCounter);
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
        keyComponents, keyBytes, keyBytesUpperBound, blockEntries, readCounter);
  }

  /** Returns whether this ISM reader has been initialized. */
  @Override
  public boolean isInitialized() {
    return footer != null;
  }

  @Override
  public boolean isEmpty() throws IOException {
    SideInputReadCounter readCounter = IsmReader.getCurrentSideInputCounter();
    closeIfPresent(
        initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent(), readCounter));
    return footer.getNumberOfKeys() == 0;
  }

  @Override
  public ResourceId getResourceId() {
    return resourceId;
  }

  @Override
  public IsmRecordCoder<V> getCoder() {
    return coder;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(IsmReaderImpl.class)
        .add("resource id", resourceId)
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
  private synchronized Optional<SeekableByteChannel> initializeFooterAndShardIndex(
      Optional<SeekableByteChannel> inChannel, SideInputReadCounter readCounter)
      throws IOException {
    if (footer != null) {
      checkState(
          shardIdToShardMap != null, "Expected shard id to shard map to have been initialized.");
      checkState(
          shardOffsetToShardMap != null,
          "Expected shard offset to shard map to have been initialized.");
      return inChannel;
    }
    checkState(
        shardIdToShardMap == null, "Expected shard id to shard map to not have been initialized.");
    checkState(
        shardOffsetToShardMap == null,
        "Expected shard offset to shard map to not have been initialized.");

    SeekableByteChannel rawChannel;
    RandomAccessData data;
    long startPosition;
    try (Closeable closeReadCounter = readCounter.enter()) {
      rawChannel = openIfNeeded(inChannel);
      this.length = rawChannel.size();

      // We read the last chunk of data, for small files we will capture the entire file.
      // We may capture the Bloom filter, shard index, and footer for slightly larger files.
      // Otherwise we are guaranteed to capture the footer and the shard index.
      startPosition = Math.max(length - MAX_SHARD_INDEX_AND_FOOTER_SIZE, 0);
      position(rawChannel, startPosition);
      data = new RandomAccessData(ByteStreams.toByteArray(Channels.newInputStream(rawChannel)));
    }
    readCounter.addBytesRead(data.size());
    // Read the fixed length footer.
    this.footer =
        FooterCoder.of()
            .decode(data.asInputStream(data.size() - Footer.FIXED_LENGTH, Footer.FIXED_LENGTH));

    checkState(
        startPosition < footer.getIndexPosition(),
        "Malformed file, expected to have been able to read entire shard index.");
    int offsetWithinReadData = (int) (footer.getIndexPosition() - startPosition);

    // Decode the list of Ism shard descriptors
    List<IsmShard> ismShards =
        IsmFormat.ISM_SHARD_INDEX_CODER.decode(
            data.asInputStream(offsetWithinReadData, data.size() - offsetWithinReadData));

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
      Optional<SeekableByteChannel> cachedDataChannel =
          Optional.<SeekableByteChannel>of(
              new CachedTailSeekableByteChannel(startPosition, data.array()));
      initializeBloomFilterAndIndexPerShard(cachedDataChannel);

      // For small files, we may have read the whole thing during initialization so
      // lets cache all this information. Note that this is important for the many small files
      // case since the IsmSideInputReader does initialization in parallel.
      if (cache != null && startPosition == 0) {
        for (IsmShard ismShard : ismShards) {
          initializeForKeyedRead(ismShard.getId(), cachedDataChannel, readCounter);
        }
        for (SortedMap<RandomAccessData, IsmShardKey> shards : indexPerShard.values()) {
          for (Map.Entry<RandomAccessData, IsmShardKey> block : shards.entrySet()) {
            cache.put(
                block.getValue(),
                new IsmCacheLoader(block.getValue()).call(cachedDataChannel.get()));
          }
        }
      }
    }

    return Optional.of(rawChannel);
  }

  /**
   * Initializes the Bloom filter and index per shard. We prepopulate empty indices for shards where
   * the index offset matches the following shard block offset. Re-uses the provided channel,
   * returning it or a new one if this method was required to open one.
   */
  private synchronized Optional<SeekableByteChannel> initializeBloomFilterAndIndexPerShard(
      Optional<SeekableByteChannel> inChannel) throws IOException {
    if (indexPerShard != null) {
      checkState(bloomFilter != null, "Expected Bloom filter to have been initialized.");
      return inChannel;
    }

    SeekableByteChannel rawChannel = openIfNeeded(inChannel);

    // Set the position to where the bloom filter is and read it in.
    position(rawChannel, footer.getBloomFilterPosition());
    bloomFilter = ScalableBloomFilterCoder.of().decode(Channels.newInputStream(rawChannel));

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
        indexPerShard.put(
            currentShard.getId(),
            ImmutableSortedMap.<RandomAccessData, IsmShardKey>orderedBy(
                    RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR)
                .put(
                    new RandomAccessData(0),
                    new IsmShardKey(
                        IsmReaderImpl.this.resourceId.toString(),
                        new RandomAccessData(0),
                        currentShard.getBlockOffset(),
                        currentShard.getIndexOffset()))
                .build());
      }
      currentShard = nextShard;
    }

    // Add an entry for the last shard if its index offset is equivalent to the
    // start of the Bloom filter, then we know that the index is empty.
    if (currentShard.getIndexOffset() == footer.getBloomFilterPosition()) {
      indexPerShard.put(
          currentShard.getId(),
          ImmutableSortedMap.<RandomAccessData, IsmShardKey>orderedBy(
                  RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR)
              .put(
                  new RandomAccessData(0),
                  new IsmShardKey(
                      IsmReaderImpl.this.resourceId.toString(),
                      new RandomAccessData(0),
                      currentShard.getBlockOffset(),
                      currentShard.getIndexOffset()))
              .build());
    }

    return Optional.of(rawChannel);
  }

  /** A unique key used to fully describe an Ism shard. */
  static final class IsmShardKey {
    private final String resourceId;
    private final RandomAccessData firstKey;
    private final long startOffset;
    private final long endOffset;

    IsmShardKey(String resourceId, RandomAccessData firstKey, long startOffset, long endOffset) {
      this.resourceId = resourceId;
      this.firstKey = firstKey;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(IsmShardKey.class)
          .add("resource id", resourceId)
          .add("firstKey", firstKey)
          .add("startOffset", startOffset)
          .add("endOffset", endOffset)
          .toString();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof IsmShardKey)) {
        return false;
      }
      IsmShardKey key = (IsmShardKey) obj;
      return startOffset == key.startOffset
          && endOffset == key.endOffset
          && Objects.equals(resourceId, key.resourceId)
          && Objects.equals(firstKey, key.firstKey);
    }

    @Override
    public int hashCode() {
      int result = resourceId.hashCode();
      result = result * 31 + firstKey.hashCode();
      result = result * 31 + Longs.hashCode(startOffset);
      result = result * 31 + Longs.hashCode(endOffset);
      return result;
    }
  }

  /**
   * Initializes the footer, shard index, Bloom filter and index for the requested shard id if they
   * have not been initialized yet. Re-uses the provided channel, returning it or a new one if this
   * method was required to open one.
   */
  // Real bug - https://issues.apache.org/jira/browse/BEAM-6559
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  private Optional<SeekableByteChannel> initializeForKeyedRead(
      int shardId, Optional<SeekableByteChannel> inChannel, SideInputReadCounter readCounter)
      throws IOException {
    inChannel = initializeFooterAndShardIndex(inChannel, readCounter);

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

    checkState(
        indexPerShard.get(shardId) == null,
        "Expected to not have initialized index for shard %s",
        shardId);

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
            IsmReaderImpl.this.resourceId.toString(),
            new RandomAccessData(0),
            shardWithIndex.getBlockOffset(),
            currentOffset));

    // While another index entry exists, insert an index entry with the key, and offsets
    // that limit the range of the shard block.
    while (rawChannel.position() < startOfNextBlock) {
      RandomAccessData nextKeyBytes = currentKeyBytes.copy();
      readKey(inStream, nextKeyBytes);
      long nextOffset = VarInt.decodeLong(inStream);

      builder.put(
          currentKeyBytes,
          new IsmShardKey(
              IsmReaderImpl.this.resourceId.toString(),
              currentKeyBytes,
              currentOffset,
              nextOffset));

      currentKeyBytes = nextKeyBytes;
      currentOffset = nextOffset;
    }

    // Upper bound the last entry with the index offset.
    builder.put(
        currentKeyBytes,
        new IsmShardKey(
            IsmReaderImpl.this.resourceId.toString(),
            currentKeyBytes,
            currentOffset,
            shardWithIndex.getIndexOffset()));
    indexPerShard.put(shardId, builder.build());

    return Optional.of(rawChannel);
  }

  /** A function which takes an IsmShardKey fully describing a data block to read and return. */
  private class IsmCacheLoader
      implements Callable<
          WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>>> {

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
        return call(rawChannel);
      }
    }

    public WeightedValue<NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>> call(
        SeekableByteChannel rawChannel) throws IOException {
      SideInputReadCounter readCounter = IsmReader.getCurrentSideInputCounter();
      try (WithinShardIsmReaderIterator readerIterator =
          new WithinShardIsmReaderIterator(
              rawChannel, key.firstKey, key.startOffset, key.endOffset, readCounter)) {

        ImmutableSortedMap.Builder<RandomAccessData, WindowedValue<IsmRecord<V>>> mapBuilder =
            ImmutableSortedMap.orderedBy(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR);
        for (boolean more = readerIterator.start(); more; more = readerIterator.advance()) {
          RandomAccessData nextKey = readerIterator.getCurrentKeyBytes().copy();
          WindowedValue<IsmRecord<V>> next = readerIterator.getCurrent();
          mapBuilder.put(nextKey, next);
        }
        // We return the size of the data block as the weight of this data block.
        return WeightedValue.of(
            (NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>) mapBuilder.build(),
            key.endOffset - key.startOffset);
      }
    }
  }

  /**
   * Fetches the data block requested.
   *
   * <p>If the cache is available, we will load and cache the requested block. Otherwise, we will
   * load and return the block.
   */
  private NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> fetch(IsmShardKey key)
      throws IOException {
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
  abstract class IsmPrefixReaderIteratorImpl extends IsmPrefixReaderIterator {
    private final List<?> keyComponents;
    final SideInputReadCounter readCounter;

    private IsmPrefixReaderIteratorImpl(List<?> keyComponents, SideInputReadCounter readCounter) {
      this.keyComponents = keyComponents;
      this.readCounter = readCounter;
    }

    /** Returns the list of key components representing this iterators key prefix. */
    protected List<?> getKeyComponents() {
      return keyComponents;
    }

    /**
     * Concatenates this reader iterators key components with the additionally supplied key
     * components and encodes them into their byte representations producing a key. Returns the
     * exact record represented by the key generated above.
     *
     * <p>Null is returned if no key has all the key components as a prefix within this file.
     */
    @Override
    public final WindowedValue<IsmRecord<V>> get(List<?> additionalKeyComponents)
        throws IOException {
      RandomAccessData keyBytes = new RandomAccessData();
      try (Closeable readerCloser = readCounter.enter()) {
        int shardId =
            coder.encodeAndHash(
                ImmutableList.builder()
                    .addAll(keyComponents)
                    .addAll(additionalKeyComponents)
                    .build(),
                keyBytes);
        return getBlock(keyBytes, shardId, readCounter).get(keyBytes);
      }
    }

    /**
     * Returns the record for the last key having this iterators key prefix. Last is defined as the
     * largest key with the same key prefix when comparing key's byte representations using an
     * unsigned lexicographical byte order.
     *
     * <p>Null is returned if the prefix is not present within this file.
     */
    @Override
    public WindowedValue<IsmRecord<V>> getLast() throws IOException {
      RandomAccessData keyBytes = new RandomAccessData();
      int shardId = coder.encodeAndHash(keyComponents, keyBytes);

      Optional<SeekableByteChannel> inChannel =
          initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent(), readCounter);

      // Key is not stored here
      if (!shardIdToShardMap.containsKey(shardId) || !bloomFilterMightContain(keyBytes)) {
        return null;
      }

      inChannel = initializeForKeyedRead(shardId, inChannel, readCounter);
      closeIfPresent(inChannel);

      final NavigableMap<RandomAccessData, IsmShardKey> indexInShard = indexPerShard.get(shardId);
      RandomAccessData end = keyBytes.increment();
      final IsmShardKey cacheEntry = indexInShard.floorEntry(end).getValue();

      NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> block;
      try (Closeable readerCloser = IsmReader.setSideInputReadContext(readCounter)) {
        block = fetch(cacheEntry);
      }

      RandomAccessData lastKey = block.lastKey();

      // If the requested key is greater than the last key within the block, then it
      // does not exist.
      if (RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(keyBytes, lastKey) > 0) {
        return null;
      }

      Entry<RandomAccessData, WindowedValue<IsmRecord<V>>> rval = block.floorEntry(end);

      // If the prefix matches completely then we can return
      if (RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.commonPrefixLength(
              keyBytes, rval.getKey())
          == keyBytes.size()) {
        return rval.getValue();
      }
      return null;
    }
  }

  /** An empty reader iterator. */
  class EmptyIsmPrefixReaderIterator extends IsmPrefixReaderIteratorImpl {
    private EmptyIsmPrefixReaderIterator(List<?> keyComponents) {
      super(keyComponents, NoopSideInputReadCounter.INSTANCE);
    }

    @Override
    public boolean start() throws IOException {
      return false;
    }

    @Override
    public boolean advance() throws IOException {
      return false;
    }

    @Override
    public WindowedValue<IsmRecord<V>> getCurrent() throws NoSuchElementException {
      throw new NoSuchElementException();
    }
  }

  /** A reader iterator which initializes its input stream lazily. */
  class LazyIsmPrefixReaderIterator extends IsmPrefixReaderIteratorImpl {
    private IsmPrefixReaderIterator delegate;

    public LazyIsmPrefixReaderIterator(SideInputReadCounter readCounter) {
      super(ImmutableList.of(), readCounter);
    }

    @Override
    public boolean start() throws IOException {
      checkState(delegate == null, "Already started");
      try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
        delegate = overKeyComponents(getKeyComponents());
      }
      return delegate.start();
    }

    @Override
    public boolean advance() throws IOException {
      checkState(delegate != null, "Not started");
      return delegate.advance();
    }

    @Override
    public WindowedValue<IsmRecord<V>> getCurrent() {
      checkState(delegate != null, "Not started");
      return delegate.getCurrent();
    }

    @Override
    public void close() throws IOException {
      if (delegate != null) {
        delegate.close();
      }
    }
  }

  /**
   * Returns a map from key to value, where the keys are in increasing lexicographical order. If the
   * requested key is not contained within this file, an empty map is returned.
   */
  private NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> getBlock(
      RandomAccessData keyBytes, int shardId, SideInputReadCounter readCounter) throws IOException {
    Optional<SeekableByteChannel> inChannel =
        initializeFooterAndShardIndex(Optional.<SeekableByteChannel>absent(), readCounter);

    // Key is not stored here so return an empty map.
    if (!shardIdToShardMap.containsKey(shardId) || !bloomFilterMightContain(keyBytes)) {
      return ImmutableSortedMap.<RandomAccessData, WindowedValue<IsmRecord<V>>>orderedBy(
              RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR)
          .build();
    }

    inChannel = initializeForKeyedRead(shardId, inChannel, readCounter);
    closeIfPresent(inChannel);

    final NavigableMap<RandomAccessData, IsmShardKey> indexInShard = indexPerShard.get(shardId);
    final IsmShardKey cacheEntry = indexInShard.floorEntry(keyBytes).getValue();
    try (Closeable readerCloseable = IsmReader.setSideInputReadContext(readCounter)) {
      return fetch(cacheEntry);
    }
  }

  /**
   * A reader iterator that returns all elements from prefix (inclusive) to prefixUpperBound
   * (exclusive) within the set of block entries provided.
   */
  private class WithinShardIsmPrefixReaderIterator extends IsmPrefixReaderIteratorImpl {
    private final Iterator<IsmShardKey> blockEntriesIterator;
    Iterator<WindowedValue<IsmRecord<V>>> iterator;
    private final RandomAccessData prefix;
    private final RandomAccessData prefixUpperBound;
    private Optional<WindowedValue<IsmRecord<V>>> current;

    private WithinShardIsmPrefixReaderIterator(
        List<?> keyComponents,
        RandomAccessData prefix,
        RandomAccessData prefixUpperBound,
        Iterator<IsmShardKey> blockEntriesIterator,
        SideInputReadCounter readCounter) {
      super(keyComponents, readCounter);
      checkNotNull(blockEntriesIterator);
      this.prefix = prefix;
      this.prefixUpperBound = prefixUpperBound;
      this.blockEntriesIterator = blockEntriesIterator;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      // This is in a while loop because the blocks that we are asked to look into may
      // not contain the key prefix.
      while (iterator == null || !iterator.hasNext()) {
        // If there are no blocks to iterate over we can return false
        if (!blockEntriesIterator.hasNext()) {
          return false;
        }

        NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> map;
        try (Closeable counterCloseable = IsmReader.setSideInputReadContext(readCounter)) {
          IsmShardKey nextBlock = blockEntriesIterator.next();
          map = fetch(nextBlock);
        }
        SortedMap<RandomAccessData, WindowedValue<IsmRecord<V>>> submap =
            map.subMap(prefix, prefixUpperBound);
        Collection<WindowedValue<IsmRecord<V>>> values = submap.values();
        iterator = values.iterator();
      }
      current = Optional.of(iterator.next());
      return true;
    }

    @Override
    public WindowedValue<IsmRecord<V>> getCurrent() throws NoSuchElementException {
      if (!current.isPresent()) {
        throw new NoSuchElementException();
      }
      return current.get();
    }
  }

  /** A reader iterator that returns all records across all shards contained within this file. */
  private class ShardAwareIsmPrefixReaderIterator extends IsmPrefixReaderIteratorImpl {
    private final SeekableByteChannel rawChannel;
    private WithinShardIsmReaderIterator delegate;
    private Iterator<IsmShard> shardEntries;

    private ShardAwareIsmPrefixReaderIterator(
        List<?> keyComponents, SeekableByteChannel rawChannel, SideInputReadCounter readCounter)
        throws IOException {
      super(keyComponents, readCounter);
      checkState(
          shardOffsetToShardMap.size() > 0,
          "Expected that shard offset to shard map has been initialized and is not empty.");

      this.rawChannel = rawChannel;
      this.shardEntries = shardOffsetToShardMap.values().iterator();
      IsmShard firstShard = shardEntries.next();
      delegate =
          new WithinShardIsmReaderIterator(
              rawChannel,
              new RandomAccessData(),
              firstShard.getBlockOffset(),
              firstShard.getIndexOffset(),
              readCounter);
    }

    @Override
    public boolean start() throws IOException {
      checkState(delegate.start(), "Expected each shard to contain at least one entry");
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      if (delegate.advance()) {
        return true;
      }
      // If our current shard index is empty, we need to move to the next one.
      if (!shardEntries.hasNext()) {
        return false;
      }
      IsmShard nextIsmShard = shardEntries.next();
      checkState(
          nextIsmShard.getBlockOffset() >= rawChannel.position(),
          "Expected channel read order to be sequential.");

      delegate =
          new WithinShardIsmReaderIterator(
              rawChannel,
              new RandomAccessData(),
              nextIsmShard.getBlockOffset(),
              nextIsmShard.getIndexOffset(),
              readCounter);

      checkState(delegate.start(), "Expected each shard to contain at least one entry.");
      return true;
    }

    @Override
    public WindowedValue<IsmRecord<V>> getCurrent() throws NoSuchElementException {
      return delegate.getCurrent();
    }

    @Override
    public void close() throws IOException {
      rawChannel.close();
    }

    @Override
    public WindowedValue<IsmRecord<V>> getLast() throws IOException {
      // Since this is an iterator over all of the file, we return the last record within the file.
      try (Closeable readerCloser = readCounter.enter()) {
        int lastShardId = shardOffsetToShardMap.lastEntry().getValue().getId();
        initializeForKeyedRead(lastShardId, Optional.<SeekableByteChannel>absent(), readCounter);
        NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>> lastBlock =
            getBlock(indexPerShard.get(lastShardId).lastKey(), lastShardId, readCounter);
        return lastBlock.lastEntry().getValue();
      }
    }
  }

  /**
   * A reader iterator for Ism formatted files which returns a sequence of Ism records from the
   * underlying channel from where it is currently positioned till the supplied limit.
   *
   * <p>This reader iterator will use the supplied bytes as the bytes for the previous key which is
   * required to do the KeyPrefix based key decoding.
   */
  private class WithinShardIsmReaderIterator
      extends NativeReaderIterator<WindowedValue<IsmRecord<V>>> {
    private final SeekableByteChannel rawChannel;
    private final InputStream inStream;
    private final SideInputReadCounter readCounter;
    private long readLimit;
    private RandomAccessData keyBytes;
    private long position;
    private Optional<WindowedValue<IsmRecord<V>>> current;

    private WithinShardIsmReaderIterator(
        SeekableByteChannel rawChannel,
        RandomAccessData currentKeyBytes,
        long newPosition,
        long newLimit,
        SideInputReadCounter readCounter)
        throws IOException {
      checkNotNull(rawChannel);
      checkNotNull(currentKeyBytes);
      checkArgument(newLimit >= 0L);
      checkArgument(newPosition <= newLimit);
      this.rawChannel = rawChannel;
      this.inStream = Channels.newInputStream(rawChannel);
      this.keyBytes = currentKeyBytes.copy();
      this.position = newPosition;
      this.readLimit = newLimit;
      this.readCounter = readCounter;
      IsmReaderImpl.position(rawChannel, newPosition);
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException, NoSuchElementException {
      checkState(position <= readLimit, "Read past end of stream");
      if (position == readLimit) {
        current = Optional.absent();
        return false;
      }

      final IsmRecord<V> ismRecord;
      readKey(inStream, keyBytes);
      InputStream keyInputStream = keyBytes.asInputStream(0, keyBytes.size());
      List<Object> keyComponents = new ArrayList<>(coder.getKeyComponentCoders().size());
      for (int i = 0; i < coder.getKeyComponentCoders().size(); ++i) {
        keyComponents.add(coder.getKeyComponentCoder(i).decode(keyInputStream));
      }

      if (IsmFormat.isMetadataKey(keyComponents)) {
        byte[] metadata = ByteArrayCoder.of().decode(inStream);
        ismRecord = IsmRecord.<V>meta(keyComponents, metadata);
      } else {
        V value = coder.getValueCoder().decode(inStream);
        ismRecord = IsmRecord.<V>of(keyComponents, value);
      }

      long newPosition = rawChannel.position();
      IsmReaderImpl.this.notifyElementRead(newPosition - position);
      readCounter.addBytesRead(newPosition - position);
      position = newPosition;
      current = Optional.of(new ValueInEmptyWindows<>(ismRecord));
      return true;
    }

    @Override
    public WindowedValue<IsmRecord<V>> getCurrent() {
      if (!current.isPresent()) {
        throw new NoSuchElementException();
      }
      return current.get();
    }

    public RandomAccessData getCurrentKeyBytes() {
      if (!current.isPresent()) {
        throw new NoSuchElementException();
      }
      return keyBytes;
    }
  }

  /**
   * Decodes a KeyPrefix from the stream and then reads unshared key bytes from the stream placing
   * them into the supplied keyBytes at position keyBytes[shared key bytes : shared key bytes +
   * unshared key bytes].
   */
  private static void readKey(InputStream inStream, RandomAccessData keyBytes) throws IOException {
    KeyPrefix keyPrefix = KeyPrefixCoder.of().decode(inStream);
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
  private SeekableByteChannel open() throws IOException {
    ReadableByteChannel channel = FileSystems.open(resourceId);
    Preconditions.checkArgument(
        channel instanceof SeekableByteChannel,
        "IsmReaderImpl requires a SeekableByteChannel for path %s but received %s.",
        resourceId,
        channel);
    return (SeekableByteChannel) channel;
  }

  /**
   * Seeks into the channel intelligently by either resetting the position or reading and discarding
   * bytes.
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

  /**
   * A {@link SeekableByteChannel} which uses a cached data segment representing the tail of a
   * {@link ReadableByteChannel}. Note that this channel only supports read operations. Closing this
   * channel is a no-op.
   */
  static class CachedTailSeekableByteChannel implements SeekableByteChannel {
    final long offset;
    final byte[] data;
    // Represents the relative position inside of data
    int localPosition;

    CachedTailSeekableByteChannel(long offset, byte[] data) {
      checkArgument(offset >= 0, "Offset must be non-negative.");
      checkNotNull(data, "Cached data must not be null.");
      this.offset = offset;
      this.data = data;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (localPosition >= data.length) {
        return -1;
      }
      int length = Math.min(dst.remaining(), data.length - localPosition);
      dst.put(data, localPosition, length);
      localPosition += length;
      return length;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
      // The external position is our relative position inside of data plus the offset.
      return offset + localPosition;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      checkArgument(
          newPosition >= offset && newPosition <= offset + data.length,
          "Cannot seek to position %s which is outside of cached data range [%s, %s].",
          newPosition,
          offset,
          offset + data.length);
      localPosition = Ints.checkedCast(newPosition - offset);
      return this;
    }

    @Override
    public long size() throws IOException {
      // The external size is the offset plus the amount of data that we are caching.
      return offset + data.length;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      throw new NonWritableChannelException();
    }
  }
}
