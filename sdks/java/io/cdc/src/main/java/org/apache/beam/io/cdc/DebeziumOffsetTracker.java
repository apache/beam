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

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link RestrictionTracker} for Debezium connectors
 */
public class DebeziumOffsetTracker extends RestrictionTracker<DebeziumOffsetHolder, Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumOffsetTracker.class);

    private DebeziumOffsetHolder restriction;
    private static final long MILLIS = 60 * 1000;
    private final Integer maxRecords = 10;

    DebeziumOffsetTracker(DebeziumOffsetHolder holder) {
        this.restriction = holder;
    }

    /**
     * Overriding {@link #tryClaim} in order to stop fetching records from the database.
     *
     * <p>This works on two different ways:</p>
     * <h3>Number of records</h3>
     * <p>
     *     This is the default behavior.
     *     Once the specified number of records has been reached, it will stop fetching them.
     * </p>
     * <h3>Time based</h3>
     * </p>
     *     User may specify the amount of time the connector to be kept alive.
     *     Please see {@link KafkaSourceConsumerFn} for more details on this.
     * </p>
     *
     *
     * @param position Currently not used
     * @return boolean
     */
    @Override
    public boolean tryClaim(Map<String, Object> position) {
        LOG.debug("-------------- Claiming {} used to have: {}", position, restriction.offset);
        long elapsedTime = System.currentTimeMillis() - KafkaSourceConsumerFn.startTime.getMillis();
        int fetchedRecords = this.restriction.fetchedRecords == null ? 0 : this.restriction.fetchedRecords + 1;
        LOG.debug("-------------- Time running: {} / {}", elapsedTime, (KafkaSourceConsumerFn.minutesToRun * MILLIS));
        this.restriction = new DebeziumOffsetHolder(position, this.restriction.history, fetchedRecords);
        LOG.debug("-------------- History: {}", this.restriction.history);
        if (KafkaSourceConsumerFn.minutesToRun < 0) {
            return fetchedRecords < maxRecords;
        }
        return elapsedTime < (KafkaSourceConsumerFn.minutesToRun * MILLIS);
    }
    
    @Override
    public DebeziumOffsetHolder currentRestriction() {
        return restriction;
    }

    @Override
    public SplitResult<DebeziumOffsetHolder> trySplit(double fractionOfRemainder) {
        LOG.debug("-------------- Trying to split: fractionOfRemainder={}", fractionOfRemainder);

        return SplitResult.of(new DebeziumOffsetHolder(null, null, null), restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {
    }

    @Override
    public IsBounded isBounded() {
        return IsBounded.BOUNDED;
    }
}
