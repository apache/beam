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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceEnumeratorState.AssignmentMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Serializes source enumerator state and upgrades the map used by earlier runner versions. */
final class FlinkSourceEnumeratorStateSerializer<T>
    implements SimpleVersionedSerializer<FlinkSourceEnumeratorState<T>> {
  // Version written by SerdeUtils.getNaiveObjectSerializer, which serialized the
  // Map<Integer, List<FlinkSourceSplit<T>>> state used by earlier runner versions.
  static final int LEGACY_MAP_VERSION = 0;
  static final int VERSION = 1;

  private final AssignmentMode legacyAssignmentMode;

  FlinkSourceEnumeratorStateSerializer(AssignmentMode legacyAssignmentMode) {
    this.legacyAssignmentMode = legacyAssignmentMode;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(FlinkSourceEnumeratorState<T> state) throws IOException {
    return SerdeUtils.serializeObject(state);
  }

  @Override
  @SuppressWarnings("unchecked")
  public FlinkSourceEnumeratorState<T> deserialize(int version, byte[] serialized)
      throws IOException {
    if (version == VERSION) {
      Object deserialized = SerdeUtils.deserializeObject(serialized);
      if (deserialized instanceof FlinkSourceEnumeratorState) {
        return (FlinkSourceEnumeratorState<T>) deserialized;
      }
      throw new IOException(
          "Expected source enumerator state of type FlinkSourceEnumeratorState for version "
              + version
              + ", but got: "
              + describe(deserialized));
    }
    if (version == LEGACY_MAP_VERSION) {
      Object deserialized = SerdeUtils.deserializeObject(serialized);
      if (deserialized instanceof Map) {
        return upgradeLegacyState((Map<?, ?>) deserialized);
      }
      throw new IOException(
          "Expected legacy source enumerator state of type Map for version "
              + version
              + ", but got: "
              + describe(deserialized));
    }
    throw new IOException(
        String.format(
            "Received source enumerator state version %d, but the highest supported version "
                + "is %d.",
            version, VERSION));
  }

  @SuppressWarnings("unchecked")
  private FlinkSourceEnumeratorState<T> upgradeLegacyState(Map<?, ?> legacyState)
      throws IOException {
    ArrayList<FlinkSourceSplit<T>> pendingSplits = new ArrayList<>();
    for (Object value : legacyState.values()) {
      List<FlinkSourceSplit<T>> legacySplits = (List<FlinkSourceSplit<T>>) value;
      if (legacySplits == null) {
        throw new IOException("Legacy source enumerator state contains a null split list.");
      }
      pendingSplits.addAll(legacySplits);
    }
    return new FlinkSourceEnumeratorState<>(legacyAssignmentMode, pendingSplits);
  }

  private static String describe(@Nullable Object obj) {
    return obj == null ? "null" : obj.getClass().getName();
  }
}
