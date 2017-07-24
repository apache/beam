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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.KeyGroupCheckpointedOperator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.joda.time.Duration;

/**
 * Remove values with duplicate ids.
 */
public class DedupingOperator<T> extends AbstractStreamOperator<WindowedValue<T>>
    implements OneInputStreamOperator<WindowedValue<ValueWithRecordId<T>>, WindowedValue<T>>,
    KeyGroupCheckpointedOperator {

  private static final long MAX_RETENTION_SINCE_ACCESS = Duration.standardMinutes(10L).getMillis();
  private static final long MAX_CACHE_SIZE = 100_000L;

  private transient LoadingCache<Integer, LoadingCache<ByteBuffer, AtomicBoolean>> dedupingCache;
  private transient KeyedStateBackend<ByteBuffer> keyedStateBackend;

  @Override
  public void open() throws Exception {
    super.open();
    checkInitCache();
    keyedStateBackend = getKeyedStateBackend();
  }

  private void checkInitCache() {
    if (dedupingCache == null) {
      dedupingCache = CacheBuilder.newBuilder().build(new KeyGroupLoader());
    }
  }

  private static class KeyGroupLoader extends
      CacheLoader<Integer, LoadingCache<ByteBuffer, AtomicBoolean>> {
    @Override
    public LoadingCache<ByteBuffer, AtomicBoolean> load(Integer ignore) throws Exception {
      return CacheBuilder.newBuilder()
          .expireAfterAccess(MAX_RETENTION_SINCE_ACCESS, TimeUnit.MILLISECONDS)
          .maximumSize(MAX_CACHE_SIZE).build(new TrueBooleanLoader());
    }
  }

  private static class TrueBooleanLoader extends CacheLoader<ByteBuffer, AtomicBoolean> {
    @Override
    public AtomicBoolean load(ByteBuffer ignore) throws Exception {
      return new AtomicBoolean(true);
    }
  }

  @Override
  public void processElement(
      StreamRecord<WindowedValue<ValueWithRecordId<T>>> streamRecord) throws Exception {
    ByteBuffer currentKey = keyedStateBackend.getCurrentKey();
    int groupIndex = keyedStateBackend.getCurrentKeyGroupIndex();
    if (shouldOutput(groupIndex, currentKey)) {
      WindowedValue<ValueWithRecordId<T>> value = streamRecord.getValue();
      output.collect(streamRecord.replace(value.withValue(value.getValue().getValue())));
    }
  }

  private boolean shouldOutput(int groupIndex, ByteBuffer id) throws ExecutionException {
    return dedupingCache.get(groupIndex).getUnchecked(id).getAndSet(false);
  }

  @Override
  public void restoreKeyGroupState(int keyGroupIndex, DataInputStream in) throws Exception {
    checkInitCache();
    Integer size = VarIntCoder.of().decode(in, Context.NESTED);
    for (int i = 0; i < size; i++) {
      byte[] idBytes = ByteArrayCoder.of().decode(in, Context.NESTED);
      // restore the ids which not expired.
      shouldOutput(keyGroupIndex, ByteBuffer.wrap(idBytes));
    }
  }

  @Override
  public void snapshotKeyGroupState(int keyGroupIndex, DataOutputStream out) throws Exception {
    Set<ByteBuffer> ids = dedupingCache.get(keyGroupIndex).asMap().keySet();
    VarIntCoder.of().encode(ids.size(), out, Context.NESTED);
    for (ByteBuffer id : ids) {
      ByteArrayCoder.of().encode(id.array(), out, Context.NESTED);
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // copy from AbstractStreamOperator
    if (getKeyedStateBackend() != null) {
      KeyedStateCheckpointOutputStream out;

      try {
        out = context.getRawKeyedOperatorStateOutput();
      } catch (Exception exception) {
        throw new Exception("Could not open raw keyed operator state stream for "
            + getOperatorName() + '.', exception);
      }

      try {
        KeyGroupsList allKeyGroups = out.getKeyGroupList();
        for (int keyGroupIdx : allKeyGroups) {
          out.startNewKeyGroup(keyGroupIdx);

          DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);

          // if (this instanceof KeyGroupCheckpointedOperator)
          snapshotKeyGroupState(keyGroupIdx, dov);

        }
      } catch (Exception exception) {
        throw new Exception("Could not write timer service of " + getOperatorName()
            + " to checkpoint state stream.", exception);
      } finally {
        try {
          out.close();
        } catch (Exception closeException) {
          LOG.warn("Could not close raw keyed operator state stream for {}. This "
                  + "might have prevented deleting some state data.", getOperatorName(),
              closeException);
        }
      }
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    if (getKeyedStateBackend() != null) {
      KeyGroupsList localKeyGroupRange = getKeyedStateBackend().getKeyGroupRange();

      for (KeyGroupStatePartitionStreamProvider streamProvider : context.getRawKeyedStateInputs()) {
        DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(streamProvider.getStream());

        int keyGroupIdx = streamProvider.getKeyGroupId();
        checkArgument(localKeyGroupRange.contains(keyGroupIdx),
            "Key Group " + keyGroupIdx + " does not belong to the local range.");

        // if (this instanceof KeyGroupRestoringOperator)
        restoreKeyGroupState(keyGroupIdx, div);

      }
    }
  }

}
