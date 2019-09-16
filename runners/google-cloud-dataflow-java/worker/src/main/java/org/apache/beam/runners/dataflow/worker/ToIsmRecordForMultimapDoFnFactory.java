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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * A {@link ParDoFnFactory} that creates a system {@link ParDoFn} responsible for transforming
 * {@code WindowedValue<KV<VarInt32, KV<K, V>>>} into {@code WindowedValue<IsmRecord<V>>} for for
 * writing to an ISM sink.
 *
 * <p>This {@link ParDoFnFactory} is part of an expansion of steps required to materialize ISM
 * files. See <a href="go/dataflow-side-inputs">go/dataflow-side-inputs</a> for further details.
 */
public class ToIsmRecordForMultimapDoFnFactory implements ParDoFnFactory {

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
        "Expected to received an instanceof an %s but got %s",
        IsmRecordCoder.class.getSimpleName(),
        coder);
    IsmRecordCoder<?> ismRecordCoder = (IsmRecordCoder<?>) coder;
    return new ToIsmRecordForMultimapParDoFn(
        KvCoder.of(
            ismRecordCoder.getCoderArguments().get(0), ismRecordCoder.getCoderArguments().get(1)));
  }

  private static class ToIsmRecordForMultimapParDoFn<K, W extends BoundedWindow, V>
      implements ParDoFn {
    private final Coder<KV<K, W>> sortKeyCoder;
    private Receiver receiver;

    private ToIsmRecordForMultimapParDoFn(Coder<KV<K, W>> sortKeyCoder) {
      this.sortKeyCoder = sortKeyCoder;
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
      WindowedValue<KV<Integer, Iterable<KV<KV<K, W>, V>>>> elem = (WindowedValue) untypedElem;
      Iterator<KV<KV<K, W>, V>> iterator = elem.getValue().getValue().iterator();
      long currentKeyIndex = 0;

      KV<KV<K, W>, V> currentValue = iterator.next();
      Object currentKeyStructuralValue = sortKeyCoder.structuralValue(currentValue.getKey());

      // For each element construct an ISM record keeping track of the index of the record with
      // regards to the window.
      while (iterator.hasNext()) {
        KV<KV<K, W>, V> nextValue = iterator.next();
        Object nextKeyStructuralValue = sortKeyCoder.structuralValue(nextValue.getKey());

        receiver.process(
            elem.withValue(
                IsmRecord.of(
                    ImmutableList.of(
                        currentValue.getKey().getKey(),
                        currentValue.getKey().getValue(),
                        currentKeyIndex),
                    currentValue.getValue())));

        final long nextKeyIndex;
        if (!currentKeyStructuralValue.equals(nextKeyStructuralValue)) {
          // If its a new user key or window then we reset our unique key counter to zero.
          nextKeyIndex = 0L;
        } else {
          // It is not a new user key or window then just increase the key index.
          nextKeyIndex = currentKeyIndex + 1L;
        }

        currentValue = nextValue;
        currentKeyStructuralValue = nextKeyStructuralValue;
        currentKeyIndex = nextKeyIndex;
      }

      receiver.process(
          elem.withValue(
              IsmRecord.of(
                  ImmutableList.of(
                      currentValue.getKey().getKey(),
                      currentValue.getKey().getValue(),
                      currentKeyIndex),
                  currentValue.getValue())));
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() {}

    @Override
    public void abort() {}
  }
}
