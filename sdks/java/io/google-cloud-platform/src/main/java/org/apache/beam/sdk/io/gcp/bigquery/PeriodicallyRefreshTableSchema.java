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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SnappyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** This transform periodically recalculates the schema for each destination table. */
@SuppressWarnings({"unused"})
class PeriodicallyRefreshTableSchema<DestinationT, ElementT>
    extends PTransform<
        PCollection<KV<DestinationT, ElementT>>,
        PCollection<TimestampedValue<Map<String, String>>>> {
  private final Duration refreshDuration;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;

  public PeriodicallyRefreshTableSchema(
      Duration refreshDuration,
      DynamicDestinations<?, DestinationT> dynamicDestinations,
      BigQueryServices bqServices) {
    this.refreshDuration = refreshDuration;
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
  }

  @Override
  public PCollection<TimestampedValue<Map<String, String>>> expand(
      PCollection<KV<DestinationT, ElementT>> input) {
    PCollection<TimestampedValue<Map<String, String>>> tableSchemas =
        input
            .apply("rewindow", Window.into(new GlobalWindows()))
            .apply("dedup", ParDo.of(new DedupDoFn()))
            .apply("refresh schema", ParDo.of(new RefreshTableSchemaDoFn()))
            .apply(
                "addTrigger",
                Window.<TimestampedValue<Map<String, String>>>configure()
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes());
    tableSchemas.setCoder(
        SnappyCoder.of(
            TimestampedValue.TimestampedValueCoder.of(
                MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
    return tableSchemas;
  }

  private class DedupDoFn extends DoFn<KV<DestinationT, ElementT>, KV<Void, DestinationT>> {
    Set<DestinationT> destinations = Sets.newHashSet();

    @StartBundle
    public void startBundle() {
      destinations.clear();
    }

    @ProcessElement
    public void process(@Element KV<DestinationT, ElementT> element) {
      destinations.add(element.getKey());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      for (DestinationT destination : destinations) {
        context.output(
            KV.of(null, destination), GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
      }
    }
  }

  private class RefreshTableSchemaDoFn
      extends DoFn<KV<Void, DestinationT>, TimestampedValue<Map<String, String>>> {
    private static final String DESTINATIONS_TAG = "destinations";
    private static final String DESTINATIONS_TIMER_TIME = "dest_timer_time";
    private static final String DESTINATIONS_TIMER = "destinationsTimer";

    @StateId(DESTINATIONS_TAG)
    private final StateSpec<ValueState<Set<String>>> destinationsSpec = StateSpecs.value();

    @StateId(DESTINATIONS_TIMER_TIME)
    private final StateSpec<ValueState<Instant>> timerTimeSpec =
        StateSpecs.value(InstantCoder.of());

    @TimerId(DESTINATIONS_TIMER)
    private final TimerSpec destinationsTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private @Nullable Map<String, String> currentOutput;
    private @Nullable Instant currentOutputTs;

    private transient @Nullable BigQueryServices.DatasetService datasetServiceInternal = null;

    @StartBundle
    public void startBundle() {
      initOutputMap();
    }

    public void initOutputMap() {
      if (currentOutput == null) {
        currentOutput = Maps.newHashMap();
      } else {
        currentOutput.clear();
      }
    }

    @ProcessElement
    public void process(
        @Element KV<Void, DestinationT> destination,
        @AlwaysFetched @StateId(DESTINATIONS_TAG) ValueState<Set<String>> destinations,
        @AlwaysFetched @StateId(DESTINATIONS_TIMER_TIME) ValueState<Instant> timerTime,
        @TimerId(DESTINATIONS_TIMER) Timer destinationsTimer)
        throws IOException, InterruptedException {
      Set<String> newSet = destinations.read();
      if (newSet == null) {
        newSet = Sets.newHashSet();
      }
      if (newSet.add(dynamicDestinations.getTable(destination.getValue()).getTableSpec())) {
        destinations.write(newSet);
        if (timerTime.read() == null) {
          timerTime.write(destinationsTimer.getCurrentRelativeTime().plus(refreshDuration));
          destinationsTimer.set(Preconditions.checkStateNotNull(timerTime.read()));
        }
      }
    }

    @OnTimer(DESTINATIONS_TIMER)
    public void onTimer(
        @AlwaysFetched @StateId(DESTINATIONS_TAG) ValueState<Set<String>> destinations,
        @AlwaysFetched @StateId(DESTINATIONS_TIMER_TIME) ValueState<Instant> timerTime,
        @TimerId(DESTINATIONS_TIMER) Timer destinationsTimer,
        OnTimerContext onTimerContext,
        PipelineOptions pipelineOptions)
        throws IOException, InterruptedException {
      Map<String, String> outputMap = Preconditions.checkStateNotNull(currentOutput);
      currentOutputTs = timerTime.read();

      Set<String> allDestinations = destinations.read();
      BigQueryServices.DatasetService datasetService = getDatasetService(pipelineOptions);
      for (String tableSpec : allDestinations) {
        TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
        Table table = datasetService.getTable(tableReference);
        if (table != null) {
          outputMap.put(tableSpec, BigQueryHelpers.toJsonString(table.getSchema()));
        }
      }

      timerTime.write(timerTime.read().plus(refreshDuration));
      destinationsTimer.set(timerTime.read());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      Map<String, String> outputMap = Preconditions.checkStateNotNull(currentOutput);
      if (!outputMap.isEmpty()) {
        c.output(
            TimestampedValue.of(outputMap, Preconditions.checkStateNotNull(currentOutputTs)),
            GlobalWindow.INSTANCE.maxTimestamp(),
            GlobalWindow.INSTANCE);
      }
    }

    private BigQueryServices.DatasetService getDatasetService(PipelineOptions pipelineOptions)
        throws IOException {
      if (datasetServiceInternal == null) {
        datasetServiceInternal =
            bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
      return datasetServiceInternal;
    }

    @Teardown
    public void onTeardown() {
      try {
        if (datasetServiceInternal != null) {
          datasetServiceInternal.close();
          datasetServiceInternal = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
