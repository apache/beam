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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link ParDoFnFactory} which returns a {@link ParDoFn} that transforms all {@code
 * WindowedValue<V>} to {@code WindowedValue<KV<K, V>>} for a constant key {@code K}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PairWithConstantKeyDoFnFactory implements ParDoFnFactory {
  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    Coder<?> coder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(Structs.getObject(cloudUserFn, PropertyNames.ENCODING)));
    Object key =
        CoderUtils.decodeFromByteArray(
            coder, Structs.getBytes(cloudUserFn, WorkerPropertyNames.ENCODED_KEY));
    return new PairWithConstantKeyParDoFn(key);
  }

  /**
   * A {@link ParDoFn} that transforms all {@code WindowedValue<V>} to {@code WindowedValue<KV<K,
   * V>>} for a constant key {@code K}.
   */
  private static class PairWithConstantKeyParDoFn<K> implements ParDoFn {

    private final K key;
    private Receiver receiver;

    PairWithConstantKeyParDoFn(K key) {
      this.key = key;
    }

    @Override
    public void startBundle(Receiver... receivers) throws Exception {
      checkState(
          receivers.length == 1,
          "%s.startBundle() called with %s receivers, expected exactly 1. "
              + "This is a bug in the Dataflow service",
          getClass().getSimpleName(),
          receivers.length);
      this.receiver = receivers[0];
    }

    @Override
    public void processElement(Object untypedElem) throws Exception {
      @SuppressWarnings("unchecked")
      WindowedValue<Object> elem = (WindowedValue) untypedElem;
      receiver.process(elem.withValue(KV.of(key, elem.getValue())));
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() {}

    @Override
    public void abort() {}
  }
}
