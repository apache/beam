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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Implements a WritableByteChannel that may contain multiple output shards.
 *
 * <p>This provides {@link #writeToShard}, which takes a shard number for writing to a particular
 * shard.
 *
 * <p>The channel is considered open if all downstream channels are open, and closes all downstream
 * channels when closed.
 */
@Internal
public class ShardingWritableByteChannel implements WritableByteChannel {

  /** Special shard number that causes a write to all shards. */
  public static final int ALL_SHARDS = -2;

  private final ArrayList<WritableByteChannel> writers = new ArrayList<>();

  /** Returns the number of output shards. */
  public int getNumShards() {
    return writers.size();
  }

  /** Adds another shard output channel. */
  public void addChannel(WritableByteChannel writer) {
    writers.add(writer);
  }

  /** Returns the WritableByteChannel associated with the given shard number. */
  public WritableByteChannel getChannel(int shardNum) {
    return writers.get(shardNum);
  }

  /**
   * Writes the buffer to the given shard.
   *
   * <p>This does not change the current output shard.
   *
   * @return The total number of bytes written. If the shard number is {@link #ALL_SHARDS}, then the
   *     total is the sum of each individual shard write.
   */
  public int writeToShard(int shardNum, ByteBuffer src) throws IOException {
    if (shardNum >= 0) {
      return writers.get(shardNum).write(src);
    }

    switch (shardNum) {
      case ALL_SHARDS:
        int size = 0;
        for (WritableByteChannel writer : writers) {
          size += writer.write(src);
        }
        return size;

      default:
        throw new IllegalArgumentException("Illegal shard number: " + shardNum);
    }
  }

  /**
   * Writes a buffer to all shards.
   *
   * <p>Same as calling {@code writeToShard(ALL_SHARDS, buf)}.
   */
  @Override
  public int write(ByteBuffer src) throws IOException {
    return writeToShard(ALL_SHARDS, src);
  }

  @Override
  public boolean isOpen() {
    for (WritableByteChannel writer : writers) {
      if (!writer.isOpen()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() throws IOException {
    for (WritableByteChannel writer : writers) {
      writer.close();
    }
  }
}
