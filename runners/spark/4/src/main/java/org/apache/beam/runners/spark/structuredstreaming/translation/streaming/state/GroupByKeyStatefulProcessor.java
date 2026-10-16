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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.StatefulTaskRunner.MutableStepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.collection.Iterator;

/** Runs a {@link GroupAlsoByWindowViaWindowSetNewDoFn} in {@code transformWithState}. */
public class GroupByKeyStatefulProcessor<K, V>
    extends BeamStatefulProcessor<K, V, KV<K, Iterable<V>>> {

  private final Supplier<PipelineOptions> optionsSupplier;
  private final TupleTag<KV<K, Iterable<V>>> mainTag = new TupleTag<>("output");

  private transient @Nullable DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> runner;
  private transient @Nullable MutableStepContext stepContext;

  public GroupByKeyStatefulProcessor(
      KvCoder<K, V> inputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      Supplier<PipelineOptions> optionsSupplier) {
    super(inputCoder, windowingStrategy);
    this.optionsSupplier = optionsSupplier;
  }

  @Override
  protected void processElements(
      K key, Iterator<WindowedValue<KV<K, V>>> rows, SparkTimerInternals timerInternals) {
    DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> r = runner();
    checkStateNotNull(stepContext).set(stateInternals(key), timerInternals);

    List<WindowedValue<V>> elementValues = new ArrayList<>();
    while (rows.hasNext()) {
      WindowedValue<KV<K, V>> row = rows.next();
      elementValues.add(row.withValue(row.getValue().getValue()));
    }
    r.processElement(
        WindowedValues.valueInGlobalWindow(KeyedWorkItems.elementsWorkItem(key, elementValues)));
  }

  @Override
  protected void processTimers(K key, SparkTimerInternals timerInternals) {
    List<TimerData> dueTimers = dueEventTimeTimers(timerInternals);
    if (dueTimers.isEmpty()) {
      return;
    }
    for (TimerData timer : dueTimers) {
      timerInternals.deleteTimer(timer);
    }
    DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> r = runner();
    checkStateNotNull(stepContext).set(stateInternals(key), timerInternals);
    r.processElement(
        WindowedValues.valueInGlobalWindow(KeyedWorkItems.timersWorkItem(key, dueTimers)));
  }

  private DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> runner() {
    if (runner == null) {
      MutableStepContext ctx = new MutableStepContext();
      this.stepContext = ctx;
      WindowedValueMultiReceiver receiver = outputReceiver();
      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> gabwDoFn =
          GroupAlsoByWindowViaWindowSetNewDoFn.create(
              windowingStrategy,
              this::stateInternals,
              k -> ctx.timerInternals(),
              NullSideInputReader.empty(),
              SystemReduceFn.buffering(inputCoder.getValueCoder()),
              receiver,
              mainTag);

      Coder<KeyedWorkItem<K, V>> itemCoder =
          KeyedWorkItemCoder.of(inputCoder.getKeyCoder(), inputCoder.getValueCoder(), windowCoder);

      DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> simpleRunner =
          DoFnRunners.simpleRunner(
              optionsSupplier.get(),
              gabwDoFn,
              NullSideInputReader.empty(),
              receiver,
              mainTag,
              Collections.emptyList(),
              ctx,
              itemCoder,
              Collections.emptyMap(),
              windowingStrategy,
              DoFnSchemaInformation.create(),
              Collections.emptyMap());

      runner = DoFnRunners.lateDataDroppingRunner(simpleRunner, ctx, windowingStrategy);
    }
    return runner;
  }
}
