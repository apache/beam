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
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * A {@link ParDoFnFactory} that creates a system {@link ParDoFn} responsible for limiting the users
 * key space to a fixed key space used to limit the number of ISM shards. This is done by
 * transforming {@code WindowedValue<KV<K, V>>} into {@code WindowedValue<KV<VarInt32, KV<KV<K,
 * Window>, V>>>} by applying a hashing function on the user key {@code K} and building a sort key
 * based upon the user key {@code K} and the window.
 *
 * <p>This {@link ParDoFnFactory} is part of an expansion of steps required to materialize ISM
 * files. See <a href="go/dataflow-side-inputs">go/dataflow-side-inputs</a> for further details.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CreateIsmShardKeyAndSortKeyDoFnFactory implements ParDoFnFactory {

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
    checkState(
        coder instanceof IsmRecordCoder,
        "Expected to received an instanceof an IsmRecordCoder but got %s",
        coder);
    return new CreateIsmShardKeyAndSortKeyParDoFn((IsmRecordCoder<?>) coder);
  }

  private static class CreateIsmShardKeyAndSortKeyParDoFn<K, V> implements ParDoFn {

    private final IsmRecordCoder<?> coder;
    private Receiver receiver;

    CreateIsmShardKeyAndSortKeyParDoFn(IsmRecordCoder<?> coder) {
      this.coder = coder;
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
      WindowedValue<KV<K, V>> elem = (WindowedValue) untypedElem;

      K userKey = elem.getValue().getKey();
      V userValue = elem.getValue().getValue();
      int hashKey = coder.hash(ImmutableList.of(elem.getValue().getKey()));

      // Explode all the windows the users values are in.
      for (BoundedWindow window : elem.getWindows()) {
        KV<K, BoundedWindow> sortKey = KV.of(userKey, window);
        KV<KV<K, BoundedWindow>, V> valueWithSortKey = KV.of(sortKey, userValue);
        // Note that the shuffle writer expects a KV<PrimaryKey, KV<SortKey, Value>> when sorting.
        receiver.process(elem.withValue(KV.of(hashKey, valueWithSortKey)));
      }
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() {}

    @Override
    public void abort() {}
  }
}
