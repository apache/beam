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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.streaming.MapState;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/**
 * Minimal string keyed, byte array valued store, the single persistence primitive the Beam state
 * and timer bridges are written against.
 *
 * <p>The production implementation, {@link #of(MapState)}, is backed by exactly <b>one</b> Spark
 * {@code transformWithState} {@link MapState}. That is a requirement rather than a stylistic
 * choice. Beam's {@code ReduceFnRunner} invents state tags at runtime, for example one buffer tag
 * per active window plus the watermark hold and pane info tags that go with it, so the set of state
 * addresses is not known when {@code StatefulProcessor.init} has to declare its state variables. A
 * single map keyed by {@code namespace + tag} can express that, a fixed set of typed per
 * {@code @StateId} state variables cannot.
 *
 * <p>Deferred optimisation: for a stateful {@code ParDo} the {@code @StateId} set <i>is</i> static,
 * so those could be mapped onto one RocksDB column family per state id, which would let Spark push
 * down range scans and drop the composite key prefix. That is a performance refinement only, it
 * does not change semantics, and it is deliberately not done in this POC because the
 * group-also-by-window path has to keep using the single map anyway.
 *
 * <p>Implementations are used from executor code inside one {@code handleInputRows} or {@code
 * handleExpiredTimer} invocation and are therefore neither thread safe nor serializable.
 */
public interface BytesKV {

  /** Returns the value stored under {@code key}, or {@code null} if there is none. */
  byte @Nullable [] get(String key);

  /** Stores {@code value} under {@code key}, replacing any previous value. */
  void put(String key, byte[] value);

  /** Removes {@code key}, a no-op if it is absent. */
  void remove(String key);

  /**
   * Returns a snapshot of all entries currently in the store.
   *
   * <p>The result is materialised eagerly, callers may safely mutate the store while iterating it.
   */
  Iterable<Map.Entry<String, byte[]>> entries();

  /** Returns a {@link BytesKV} view over a Spark {@code transformWithState} {@link MapState}. */
  static BytesKV of(MapState<String, byte[]> mapState) {
    return new MapStateBytesKV(mapState);
  }

  /** A {@link BytesKV} backed by a single Spark {@link MapState}. */
  final class MapStateBytesKV implements BytesKV {
    private final MapState<String, byte[]> mapState;

    private MapStateBytesKV(MapState<String, byte[]> mapState) {
      this.mapState = mapState;
    }

    @Override
    public byte @Nullable [] get(String key) {
      return mapState.containsKey(key) ? mapState.getValue(key) : null;
    }

    @Override
    public void put(String key, byte[] value) {
      mapState.updateValue(key, value);
    }

    @Override
    public void remove(String key) {
      mapState.removeKey(key);
    }

    @Override
    public Iterable<Map.Entry<String, byte[]>> entries() {
      List<Map.Entry<String, byte[]>> all = new ArrayList<>();
      scala.collection.Iterator<Tuple2<String, byte[]>> it = mapState.iterator();
      while (it.hasNext()) {
        Tuple2<String, byte[]> next = it.next();
        all.add(new AbstractMap.SimpleImmutableEntry<>(next._1(), next._2()));
      }
      return all;
    }
  }
}
