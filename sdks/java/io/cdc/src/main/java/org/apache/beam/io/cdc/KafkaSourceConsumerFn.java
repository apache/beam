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
package org.apache.beam.io.cdc;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @param <T>
 */
public class KafkaSourceConsumerFn<T> extends DoFn<Map<String, String>, T> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceConsumerFn.class);
    public static final String BEAM_INSTANCE_PROPERTY = "beam.parent.instance";

    public static long minutesToRun = -1;
    public static DateTime startTime;

    private final Class<? extends SourceConnector> connectorClass;
    private final SourceRecordMapper<T> fn;
    protected static final Map<String, RestrictionTracker<DebeziumOffsetHolder,  Map<String, Object>>>
    restrictionTrackers = new ConcurrentHashMap<>();

    public KafkaSourceConsumerFn(Class<?> connectorClass, SourceRecordMapper<T> fn, long minutesToRun) {
        this.connectorClass = (Class<? extends SourceConnector>) connectorClass;
        this.fn = fn;
        KafkaSourceConsumerFn.minutesToRun = minutesToRun;
    }

    public KafkaSourceConsumerFn(Class<?> connectorClass, SourceRecordMapper<T> fn) {
        this.connectorClass = (Class<? extends SourceConnector>) connectorClass;
        this.fn = fn;
    }

    @GetInitialRestriction
    public DebeziumOffsetHolder getInitialRestriction(@Element Map<String, String> unused) throws IOException {
        KafkaSourceConsumerFn.startTime = new DateTime();
        return new DebeziumOffsetHolder(null, null);
    }

    @NewTracker
    public RestrictionTracker<DebeziumOffsetHolder, Map<String, Object>> newTracker(@Restriction DebeziumOffsetHolder restriction) {
        return new DebeziumOffsetTracker(restriction);
    }

    @GetRestrictionCoder
    public Coder<DebeziumOffsetHolder> getRestrictionCoder() {
        return SerializableCoder.of(DebeziumOffsetHolder.class);
    }

    @DoFn.ProcessElement
    public ProcessContinuation process(@Element Map<String, String> element,
                                       RestrictionTracker<DebeziumOffsetHolder,
                                               Map<String, Object>> tracker,
                                       OutputReceiver<T> receiver) throws Exception {
        Map<String, String> configuration = new HashMap<>(element);

        // Adding the current restriction to the class object to be found by the database history
        restrictionTrackers.put(this.getHashCode(), tracker);
        configuration.put(BEAM_INSTANCE_PROPERTY, this.getHashCode());

        SourceConnector connector = connectorClass.getDeclaredConstructor().newInstance();
        connector.start(configuration);

        SourceTask task = (SourceTask) connector.taskClass().getDeclaredConstructor().newInstance();

        task.initialize(new DebeziumBeamSourceTaskContext(tracker.currentRestriction().offset));
        task.start(connector.taskConfigs(1).get(0));

        List<SourceRecord> records = task.poll();
        if (records == null) {
            LOG.debug("----------- No records found");

            restrictionTrackers.remove(this.getHashCode());
            return ProcessContinuation.stop();
        }

        if (records.size() == 0) {
            restrictionTrackers.remove(this.getHashCode());
            return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
        }

        for (SourceRecord record : records) {
            LOG.debug("------------ Record found: {}", record);

            Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

            if (offset == null || !tracker.tryClaim(offset)) {
                restrictionTrackers.remove(this.getHashCode());
                return ProcessContinuation.stop();
            }

            T json = this.fn.mapSourceRecord(record);
            LOG.debug("****************** RECEIVED SOURCE AS JSON: {}", json);

            receiver.output(json);
        }

        LOG.debug("WE SHOULD RESUME IN A BIT!");

        restrictionTrackers.remove(this.getHashCode());
        return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
    }

    public String getHashCode() {
        return Integer.toString(System.identityHashCode(this));
    }
}
