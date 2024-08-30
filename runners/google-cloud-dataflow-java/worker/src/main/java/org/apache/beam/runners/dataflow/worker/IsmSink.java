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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
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
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;

/**
 * A {@link Sink} that writes Ism files.
 *
 * @param <V> the type of the value written to the sink
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class IsmSink<V> extends Sink<WindowedValue<IsmRecord<V>>> {
  static final int BLOCK_SIZE_BYTES = 1024 * 1024;
  private final ResourceId resourceId;
  private final IsmRecordCoder<V> coder;
  private final long bloomFilterSizeLimitBytes;

  /**
   * Produces a sink for the specified {@code filename} and {@code coder}. See {@link IsmFormat} for
   * encoded format details.
   */
  IsmSink(ResourceId resourceId, IsmRecordCoder<V> coder, long bloomFilterSizeLimitBytes) {
    IsmFormat.validateCoderIsCompatible(coder);
    this.resourceId = resourceId;
    this.coder = coder;
    this.bloomFilterSizeLimitBytes = bloomFilterSizeLimitBytes;
  }

  @Override
  public SinkWriter<WindowedValue<IsmRecord<V>>> writer() throws IOException {
    return new IsmSinkWriter(FileSystems.create(resourceId, MimeTypes.BINARY));
  }

  // Can be overridden by tests to generate files with smaller block sizes for testing.
  @VisibleForTesting
  long getBlockSize() {
    return BLOCK_SIZE_BYTES;
  }

  private class IsmSinkWriter implements SinkWriter<WindowedValue<IsmRecord<V>>> {
    private final CountingOutputStream out;
    private final RandomAccessData indexOut;
    private RandomAccessData previousKeyBytes;
    private Optional<Integer> previousShard;
    private RandomAccessData currentKeyBytes;
    private RandomAccessData lastIndexKeyBytes;
    private long lastIndexedPosition;
    private long numberOfKeysWritten;
    private SortedMap<Integer, IsmShard> shardKeyToShardMap;
    private final ScalableBloomFilter.Builder bloomFilterBuilder;

    /** Creates an IsmSinkWriter for the given channel. */
    private IsmSinkWriter(WritableByteChannel channel) {
      checkNotNull(channel);
      out = new CountingOutputStream(Channels.newOutputStream(channel));
      indexOut = new RandomAccessData();
      previousShard = Optional.absent();
      previousKeyBytes = new RandomAccessData();
      currentKeyBytes = new RandomAccessData();
      lastIndexKeyBytes = new RandomAccessData();
      bloomFilterBuilder = ScalableBloomFilter.withMaximumSizeBytes(bloomFilterSizeLimitBytes);
      shardKeyToShardMap = new TreeMap<>();
    }

    @Override
    public long add(WindowedValue<IsmRecord<V>> windowedRecord) throws IOException {
      // The windowed portion of the value is ignored.
      IsmRecord<V> record = windowedRecord.getValue();

      checkArgument(coder.getKeyComponentCoders().size() == record.getKeyComponents().size());

      List<Integer> keyOffsetPositions = new ArrayList<>();
      final int currentShard =
          coder.encodeAndHash(record.getKeyComponents(), currentKeyBytes, keyOffsetPositions);
      // Put each component of the key into the Bloom filter so that we can use the Bloom
      // filter for key prefix checks.
      for (Integer offsetPosition : keyOffsetPositions) {
        bloomFilterBuilder.put(currentKeyBytes.array(), 0, offsetPosition);
      }

      // If we are moving to another shard, finish outputting the last shard.
      if (previousShard.isPresent() && currentShard != previousShard.get()) {
        // We reset last shard to be empty.
        finishShard();
      }

      long positionOfCurrentKey = out.getCount();

      // If we are outputting our first key for this shard, then we can assume 0 bytes are saved
      // from the previous key.
      int sharedKeySize;
      if (!previousShard.isPresent()) {
        sharedKeySize = 0;
        // Create a new shard record for the current value being output validating
        // that we have never seen this shard before.
        IsmShard ismShard = IsmShard.of(currentShard, positionOfCurrentKey);
        checkState(
            shardKeyToShardMap.put(currentShard, ismShard) == null,
            "Unexpected insertion of keys %s for shard which already exists %s. "
                + "Ism files expect that all shards are written contiguously.",
            record.getKeyComponents(),
            ismShard);
      } else {
        sharedKeySize = commonPrefixLengthWithOrderCheck(previousKeyBytes, currentKeyBytes);
      }

      // Put key-value mapping record into block buffer
      int unsharedKeySize = currentKeyBytes.size() - sharedKeySize;
      KeyPrefix keyPrefix = KeyPrefix.of(sharedKeySize, unsharedKeySize);
      KeyPrefixCoder.of().encode(keyPrefix, out);
      currentKeyBytes.writeTo(out, sharedKeySize, unsharedKeySize);
      if (IsmFormat.isMetadataKey(record.getKeyComponents())) {
        ByteArrayCoder.of().encode(record.getMetadata(), out);
      } else {
        coder.getValueCoder().encode(record.getValue(), out);
      }

      // If the start of the current elements position is more than block size away from our
      // last indexed position
      if (positionOfCurrentKey > lastIndexedPosition + getBlockSize()) {
        // Note that the first key of each shard will never be in the index allowing us to ignore
        // the fact that there is an empty key and use the lastIndexKeyBytes to be the 0 byte array
        // at the start of each shard.
        int sharedIndexKeySize =
            commonPrefixLengthWithOrderCheck(lastIndexKeyBytes, currentKeyBytes);
        int unsharedIndexKeySize = currentKeyBytes.size() - sharedIndexKeySize;
        KeyPrefix indexKeyPrefix = KeyPrefix.of(sharedIndexKeySize, unsharedIndexKeySize);
        KeyPrefixCoder.of().encode(indexKeyPrefix, indexOut.asOutputStream());
        currentKeyBytes.writeTo(
            indexOut.asOutputStream(), sharedIndexKeySize, unsharedIndexKeySize);
        VarInt.encode(positionOfCurrentKey, indexOut.asOutputStream());
        lastIndexKeyBytes.resetTo(0);
        currentKeyBytes.writeTo(lastIndexKeyBytes.asOutputStream(), 0, currentKeyBytes.size());
        lastIndexedPosition = out.getCount();
      }

      // Remember the shard for the current key.
      previousShard = Optional.of(currentShard);

      // Swap the current key and the previous key, resetting the previous key to be re-used.
      RandomAccessData temp = previousKeyBytes;
      previousKeyBytes = currentKeyBytes;
      currentKeyBytes = temp;
      currentKeyBytes.resetTo(0);

      numberOfKeysWritten += 1;
      return out.getCount() - positionOfCurrentKey;
    }

    /**
     * Compute the length of the common prefix of the previous key and the given key and perform a
     * key order check. We check that the currently being inserted key is strictly greater than the
     * previous key.
     */
    private int commonPrefixLengthWithOrderCheck(
        RandomAccessData prevKeyBytes, RandomAccessData currentKeyBytes) {
      int offset =
          RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.commonPrefixLength(
              prevKeyBytes, currentKeyBytes);
      int compare =
          RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
              prevKeyBytes, currentKeyBytes, offset);
      if (compare < 0) {
        return offset;
      } else if (compare == 0) {
        throw new IllegalArgumentException(
            IsmSinkWriter.class.getSimpleName()
                + " expects keys to be written in strictly increasing order but was given "
                + prevKeyBytes
                + " as the previous key and "
                + currentKeyBytes
                + " as the current key. Expected "
                + prevKeyBytes.array()[offset + 1]
                + " <= "
                + currentKeyBytes.array()[offset + 1]
                + " at position "
                + (offset + 1)
                + ".");
      } else {
        throw new IllegalArgumentException(
            IsmSinkWriter.class.getSimpleName()
                + " expects keys to be written in strictly increasing order but was given "
                + prevKeyBytes
                + " as the previous key and "
                + currentKeyBytes
                + " as the current key. Expected length of previous key "
                + prevKeyBytes.size()
                + " <= "
                + currentKeyBytes.size()
                + " to current key.");
      }
    }

    /**
     * Outputs the end of a shard. This is done by:
     *
     * <ul>
     *   <li>updating the shard index for the current shard with the index offset
     *   <li>writing out the index for the shard
     *   <li>resetting the last indexed position
     *   <li>forgetting the last shard
     * </ul>
     */
    private void finishShard() throws IOException {
      // Update the last shard record as to the position of the index.
      IsmShard ismShard = shardKeyToShardMap.get(previousShard.get());
      shardKeyToShardMap.put(previousShard.get(), ismShard.withIndexOffset(out.getCount()));

      indexOut.writeTo(out, 0, indexOut.size());
      indexOut.resetTo(0);

      // Reset the last indexed position to the start of the next shard allowing us
      // to not need to index the first key.
      lastIndexedPosition = out.getCount();
      lastIndexKeyBytes = new RandomAccessData();

      // Clear the last shard.
      previousShard = Optional.absent();
    }

    /**
     * Completes the construction of the Ism file. This is done by:
     *
     * <ul>
     *   <li>finishing the last shard if present
     *   <li>writing out the Bloom filter
     *   <li>writing out the shard index
     *   <li>writing out the footer
     * </ul>
     *
     * @throws IOException if an underlying write fails
     */
    private void finish() throws IOException {
      // Update the last shard if at least one element was written.
      if (previousShard.isPresent()) {
        finishShard();
      }

      long startOfBloomFilter = out.getCount();
      ScalableBloomFilterCoder.of().encode(bloomFilterBuilder.build(), out);

      long startOfIndex = out.getCount();
      IsmFormat.ISM_SHARD_INDEX_CODER.encode(new ArrayList<>(shardKeyToShardMap.values()), out);

      FooterCoder.of()
          .encode(Footer.of(startOfIndex, startOfBloomFilter, numberOfKeysWritten), out);
    }

    @Override
    public void close() throws IOException {
      finish();
      out.close();
    }

    @Override
    public void abort() throws IOException {
      close();
    }
  }
}
