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

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.IntegerMean;
import com.google.api.services.dataflow.model.NameAndKind;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MonitoringInfo to CounterUpdate transformer capable to transform MeanByteCount counter. */
public class MeanByteCountMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private final SpecMonitoringInfoValidator specValidator;
  private final Map<String, NameContext> pcollectionIdToNameContext;

  /**
   * @param specValidator SpecMonitoringInfoValidator to utilize for default validation.
   * @param pcollectionIdToNameContext This mapping is utilized to generate DFE CounterUpdate name.
   */
  public MeanByteCountMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, NameContext> pcollectionIdToNameContext) {
    this.specValidator = specValidator;
    this.pcollectionIdToNameContext = pcollectionIdToNameContext;
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
    if (!urn.equals(Urns.SAMPLED_BYTE_SIZE)) {
      throw new RuntimeException(String.format("Received unexpected counter urn: %s", urn));
    }

    String type = monitoringInfo.getType();
    if (!type.equals(TypeUrns.DISTRIBUTION_INT64_TYPE)) {
      throw new RuntimeException(
          String.format(
              "Received unexpected counter type. Expected type: %s, received: %s",
              TypeUrns.DISTRIBUTION_INT64_TYPE, type));
    }

    if (!pcollectionIdToNameContext.containsKey(
        monitoringInfo.getLabelsMap().get(MonitoringInfoConstants.Labels.PCOLLECTION))) {
      return Optional.of(
          "Encountered ElementCount MonitoringInfo with unknown PCollectionId: "
              + monitoringInfo.toString());
    }

    return Optional.empty();
  }

  /**
   * Generates CounterUpdate to send to DFE based on ElementCount MonitoringInfo.
   *
   * @param monitoringInfo Monitoring info to transform.
   * @return CounterUpdate generated based on provided monitoringInfo
   */
  @Override
  public @Nullable CounterUpdate transform(MonitoringInfo monitoringInfo) {
    Optional<String> validationResult = validate(monitoringInfo);
    if (validationResult.isPresent()) {
      LOG.debug(validationResult.get());
      return null;
    }

    DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
    final String pcollectionId =
        monitoringInfo.getLabelsMap().get(MonitoringInfoConstants.Labels.PCOLLECTION);
    final String pcollectionName = pcollectionIdToNameContext.get(pcollectionId).userName();

    String counterName = pcollectionName + "-MeanByteCount";
    NameAndKind name = new NameAndKind();
    name.setName(counterName).setKind(Kind.MEAN.toString());

    return new CounterUpdate()
        .setNameAndKind(name)
        .setCumulative(true)
        .setIntegerMean(
            new IntegerMean()
                .setSum(longToSplitInt(data.sum()))
                .setCount(longToSplitInt(data.count())));
  }

  /** @return URN that this transformer can convert to {@link CounterUpdate}s. */
  public static String getSupportedUrn() {
    return Urns.SAMPLED_BYTE_SIZE;
  }
}
