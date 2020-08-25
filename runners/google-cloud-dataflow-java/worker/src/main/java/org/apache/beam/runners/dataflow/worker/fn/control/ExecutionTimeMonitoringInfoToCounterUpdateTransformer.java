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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MonitoringInfo to CounterUpdate transformer capable to transform MSec counters.
 *
 * <p>Use getSupportedUrns to get all urns this class supports.
 */
public class ExecutionTimeMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private SpecMonitoringInfoValidator specValidator;
  private Map<String, DataflowStepContext> transformIdMapping;

  @VisibleForTesting
  static final Map<String, String> URN_TO_COUNTER_NAME_MAPPING =
      ImmutableMap.<String, String>builder()
          .put(MonitoringInfoConstants.Urns.START_BUNDLE_MSECS, "start-msecs")
          .put(MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS, "process-msecs")
          .put(MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS, "finish-msecs")
          .build();

  /**
   * @param specValidator SpecMonitoringInfoValidator to utilize for default validation.
   * @param transformIdMapping Mapping of PTransform ID string to DataflowStepContext.
   */
  public ExecutionTimeMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, DataflowStepContext> transformIdMapping) {
    this.specValidator = specValidator;
    this.transformIdMapping = transformIdMapping;
  }

  /**
   * Validates provided monitoring info against specs and common safety checks.
   *
   * @param monitoringInfo to validate.
   * @return Optional.empty() all validation checks are passed. Optional with error text otherwise.
   * @throws RuntimeException if received unexpected urn.
   */
  protected Optional<String> validate(MonitoringInfo monitoringInfo) {
    Optional<String> validatorResult = specValidator.validate(monitoringInfo);
    if (validatorResult.isPresent()) {
      return validatorResult;
    }

    String urn = monitoringInfo.getUrn();
    if (!URN_TO_COUNTER_NAME_MAPPING.keySet().contains(urn)) {
      throw new RuntimeException(String.format("Received unexpected counter urn: %s", urn));
    }

    String type = monitoringInfo.getType();
    if (!type.equals(TypeUrns.SUM_INT64_TYPE)) {
      throw new RuntimeException(
          String.format(
              "Received unexpected counter type. Expected type: %s, received: %s",
              TypeUrns.SUM_INT64_TYPE, type));
    }

    final String ptransform =
        monitoringInfo.getLabelsMap().get(MonitoringInfoConstants.Labels.PTRANSFORM);
    DataflowStepContext stepContext = transformIdMapping.get(ptransform);
    if (stepContext == null) {
      return Optional.of(
          "Encountered MSec MonitoringInfo with unknown ptransformId: "
              + monitoringInfo.toString());
    }

    return Optional.empty();
  }

  @Override
  public @Nullable CounterUpdate transform(MonitoringInfo monitoringInfo) {
    Optional<String> validationResult = validate(monitoringInfo);
    if (validationResult.isPresent()) {
      LOG.debug(validationResult.get());
      return null;
    }

    long value = decodeInt64Counter(monitoringInfo.getPayload());
    String urn = monitoringInfo.getUrn();

    final String ptransform =
        monitoringInfo.getLabelsMap().get(MonitoringInfoConstants.Labels.PTRANSFORM);
    DataflowStepContext stepContext = transformIdMapping.get(ptransform);

    String counterName = URN_TO_COUNTER_NAME_MAPPING.get(urn);
    CounterStructuredNameAndMetadata name = new CounterStructuredNameAndMetadata();
    name.setName(
            new CounterStructuredName()
                .setOrigin("SYSTEM")
                .setName(counterName)
                .setOriginalStepName(stepContext.getNameContext().originalName())
                .setExecutionStepName(stepContext.getNameContext().stageName()))
        .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString()));

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(true)
        .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(value));
  }

  /** @return URNs that this transformer can convert to {@link CounterUpdate}s. */
  public static Iterable<String> getSupportedUrns() {
    return URN_TO_COUNTER_NAME_MAPPING.keySet();
  }
}
