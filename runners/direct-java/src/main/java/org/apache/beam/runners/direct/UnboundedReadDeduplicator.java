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
package org.apache.beam.runners.direct;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.SplittableParDo.PrimitiveUnboundedRead;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.joda.time.Duration;

/**
 * Provides methods to determine if a record is a duplicate within the evaluation of a {@link
 * PrimitiveUnboundedRead SplittableParDo.PrimitiveUnboundedRead} {@link PTransform}.
 */
interface UnboundedReadDeduplicator {
  /**
   * Returns true if the record with the provided ID should be output, and false if it should not be
   * because it is a duplicate.
   */
  boolean shouldOutput(byte[] recordId);

  /**
   * An {@link UnboundedReadDeduplicator} that always returns true. For use with sources do not
   * require deduplication.
   */
  class NeverDeduplicator implements UnboundedReadDeduplicator {
    /** Create a new {@link NeverDeduplicator}. */
    public static UnboundedReadDeduplicator create() {
      return new NeverDeduplicator();
    }

    @Override
    public boolean shouldOutput(byte[] recordId) {
      return true;
    }
  }

  /**
   * An {@link UnboundedReadDeduplicator} that returns true if the record ID has not been seen
   * within 10 minutes.
   */
  class CachedIdDeduplicator implements UnboundedReadDeduplicator {
    private static final ByteArrayCoder RECORD_ID_CODER = ByteArrayCoder.of();
    private static final long MAX_RETENTION_SINCE_ACCESS =
        Duration.standardMinutes(10L).getMillis();

    private final LoadingCache<StructuralKey<byte[]>, AtomicBoolean> ids;

    /** Create a new {@link CachedIdDeduplicator}. */
    public static UnboundedReadDeduplicator create() {
      return new CachedIdDeduplicator();
    }

    private CachedIdDeduplicator() {
      ids =
          CacheBuilder.newBuilder()
              .expireAfterAccess(MAX_RETENTION_SINCE_ACCESS, TimeUnit.MILLISECONDS)
              .maximumSize(100_000L)
              .build(new TrueBooleanLoader());
    }

    @Override
    public boolean shouldOutput(byte[] recordId) {
      return ids.getUnchecked(StructuralKey.of(recordId, RECORD_ID_CODER)).getAndSet(false);
    }

    private static class TrueBooleanLoader
        extends CacheLoader<StructuralKey<byte[]>, AtomicBoolean> {
      @Override
      public AtomicBoolean load(StructuralKey<byte[]> key) throws Exception {
        return new AtomicBoolean(true);
      }
    }
  }
}
