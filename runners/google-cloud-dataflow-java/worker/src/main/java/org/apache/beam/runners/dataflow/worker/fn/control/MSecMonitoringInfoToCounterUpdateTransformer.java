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

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MonitoringInfo to CounterUpdate transformer capable to transform MSec counters.
 *
 * <p>Use getSupportedUrns to get all urns this class supports.
 */
public class MSecMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private SpecMonitoringInfoValidator specValidator;
  private Map<String, DataflowStepContext> transformIdMapping;
  private Map<String, String> urnToCounterNameMapping;

  /**
   * @param specValidator SpecMonitoringInfoValidator to utilize for default validation.
   * @param transformIdMapping Mapping of PTransform ID string to DataflowStepContext.
   */
  public MSecMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, DataflowStepContext> transformIdMapping) {
    this.specValidator = specValidator;
    this.transformIdMapping = transformIdMapping;
    urnToCounterNameMapping = createKnownUrnToCounterNameMapping();
  }

  /** Allows to inject members for cleaner testing. */
  @VisibleForTesting
  protected MSecMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, DataflowStepContext> transformIdMapping,
      Map<String, String> urnToCounterNameMapping) {
    this.specValidator = specValidator;
    this.transformIdMapping = transformIdMapping;
    this.urnToCounterNameMapping = urnToCounterNameMapping;
  }

  @VisibleForTesting
  protected Map<String, String> createKnownUrnToCounterNameMapping() {
    Map<String, String> result = new HashMap<>();
    result.put(MonitoringInfoConstants.Urns.START_BUNDLE_MSECS, "start-msecs");
    result.put(MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS, "process-msecs");
    result.put(MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS, "finish-msecs");
    return result;
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
    if (!urnToCounterNameMapping.keySet().contains(urn)) {
      throw new RuntimeException(String.format("Received unexpected counter urn: %s", urn));
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
  @Nullable
  public CounterUpdate transform(MonitoringInfo monitoringInfo) {
    Optional<String> validationResult = validate(monitoringInfo);
    if (validationResult.isPresent()) {
      LOG.debug(validationResult.get());
      return null;
    }

    long value = monitoringInfo.getMetric().getCounterData().getInt64Value();
    String urn = monitoringInfo.getUrn();

    final String ptransform =
        monitoringInfo.getLabelsMap().get(MonitoringInfoConstants.Labels.PTRANSFORM);
    DataflowStepContext stepContext = transformIdMapping.get(ptransform);

    String counterName = urnToCounterNameMapping.get(urn);
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

  /** @return iterable of Urns that this transformer can convert to CounterUpdates. */
  public Iterable<String> getSupportedUrns() {
    return this.urnToCounterNameMapping.keySet();
  }
}
