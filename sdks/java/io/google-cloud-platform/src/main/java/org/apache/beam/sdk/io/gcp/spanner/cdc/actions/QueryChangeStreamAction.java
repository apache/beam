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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class QueryChangeStreamAction {

  private static final Logger LOG = LoggerFactory.getLogger(QueryChangeStreamAction.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private final ChangeStreamDao changeStreamDao;
  private final ChangeStreamRecordMapper changeStreamRecordMapper;
  private final DataChangeRecordAction dataChangeRecordAction;
  private final HeartbeatRecordAction heartbeatRecordAction;
  private final ChildPartitionsRecordAction childPartitionsRecordAction;
  private final int maxCountForResuming = 5000;

  public QueryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    this.changeStreamDao = changeStreamDao;
    this.changeStreamRecordMapper = changeStreamRecordMapper;
    this.dataChangeRecordAction = dataChangeRecordAction;
    this.heartbeatRecordAction = heartbeatRecordAction;
    this.childPartitionsRecordAction = childPartitionsRecordAction;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    try (Scope scope =
        TRACER.spanBuilder("QueryChangeStreamAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String token = partition.getPartitionToken();
      try (ChangeStreamResultSet resultSet =
          changeStreamDao.changeStreamQuery(
              token,
              tracker.currentRestriction().getStartTimestamp(),
              partition.isInclusiveStart(),
              partition.getEndTimestamp(),
              partition.isInclusiveEnd(),
              partition.getHeartbeatMillis())) {
        int count = 0;
        while (resultSet.next()) {
          // TODO: Check what should we do if there is an error here
          final List<ChangeStreamRecord> records =
              changeStreamRecordMapper.toChangeStreamRecords(
                  partition.getPartitionToken(),
                  resultSet.getCurrentRowAsStruct(),
                  resultSet.getMetadata(),
                  tracker.currentRestriction().getMetadata());

          Optional<ProcessContinuation> maybeContinuation;
          for (final ChangeStreamRecord record : records) {
            if (record instanceof DataChangeRecord) {
              maybeContinuation =
                  dataChangeRecordAction.run(
                      partition, (DataChangeRecord) record, tracker, receiver, watermarkEstimator);
            } else if (record instanceof HeartbeatRecord) {
              maybeContinuation =
                  heartbeatRecordAction.run(
                      partition, (HeartbeatRecord) record, tracker, watermarkEstimator);
            } else if (record instanceof ChildPartitionsRecord) {
              maybeContinuation =
                  childPartitionsRecordAction.run(
                      partition, (ChildPartitionsRecord) record, tracker, watermarkEstimator);
            } else {
              LOG.error("[" + token + "] Unknown record type " + record.getClass());
              // FIXME: Check what should we do if the record is unknown
              throw new IllegalArgumentException("Unknown record type " + record.getClass());
            }
            if (maybeContinuation.isPresent()) {
              LOG.info("[" + token + "] Continuation present, returning " + maybeContinuation);
              return maybeContinuation;
            }
          }

          // TODO: this is a temporary solution to solve the issue that if the
          // function does not resume, the system watermark seems not to be
          // updated and the downstream windows would not work.
          if (count >= maxCountForResuming) {
            return Optional.of(ProcessContinuation.resume());
          }
          count++;
        }
        LOG.debug("[" + token + "] = Query change stream action completed successfully");
        return Optional.empty();
      }
    }
  }
}
