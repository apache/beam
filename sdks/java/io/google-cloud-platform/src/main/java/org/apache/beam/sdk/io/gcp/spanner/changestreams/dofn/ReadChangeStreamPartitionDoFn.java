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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.actions.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.actions.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.actions.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.LenientOffsetRangeTracker;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private final ChangeStreamMetrics metrics;

  private transient QueryChangeStreamAction queryChangeStreamAction;

  public ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
    this.metrics = metrics;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element PartitionMetadata partition) {
    final String token = partition.getPartitionToken();
    final com.google.cloud.Timestamp startTimestamp = partition.getStartTimestamp();
    final long startMicros = TimestampConverter.timestampToMicros(startTimestamp);
    // Offset range represents closed-open interval
    final long endMicros =
        Optional.ofNullable(partition.getEndTimestamp())
            .map(TimestampConverter::timestampToMicros)
            .map(micros -> micros + 1)
            .orElse(TimestampConverter.MAX_MICROS);
    final com.google.cloud.Timestamp partitionScheduledAt = partition.getScheduledAt();

    final com.google.cloud.Timestamp partitionRunningAt =
        daoFactory.getPartitionMetadataDao().updateToRunning(token);
    metrics.updatePartitionScheduledToRunning(
        new Duration(
            partitionScheduledAt.toSqlTimestamp().getTime(),
            partitionRunningAt.toSqlTimestamp().getTime()));

    return new OffsetRange(startMicros, endMicros);
  }

  @NewTracker
  public LenientOffsetRangeTracker newTracker(
      @Element PartitionMetadata partition, @Restriction OffsetRange offsetRange) {
    return new LenientOffsetRangeTracker(partition, offsetRange);
  }

  @Setup
  public void setup() {
    final PartitionMetadataDao partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    final ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
    final ChangeStreamRecordMapper changeStreamRecordMapper =
        mapperFactory.changeStreamRecordMapper();
    final DataChangeRecordAction dataChangeRecordAction = actionFactory.dataChangeRecordAction();
    final HeartbeatRecordAction heartbeatRecordAction =
        actionFactory.heartbeatRecordAction(metrics);
    final ChildPartitionsRecordAction childPartitionsRecordAction =
        actionFactory.childPartitionsRecordAction(partitionMetadataDao, metrics);

    this.queryChangeStreamAction =
        actionFactory.queryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction);
  }

  // TODO: Close DAOs on teardown
  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BundleFinalizer bundleFinalizer) {

    final String token = partition.getPartitionToken();
    try (Scope scope =
        TRACER
            .spanBuilder("ReadChangeStreamPartitionDoFn.processElement")
            .setRecordEvents(true)
            .startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(token));

      LOG.debug(
          "[" + token + "] Processing element with restriction " + tracker.currentRestriction());

      return queryChangeStreamAction.run(
          partition, tracker, receiver, watermarkEstimator, bundleFinalizer);
    }
  }
}
