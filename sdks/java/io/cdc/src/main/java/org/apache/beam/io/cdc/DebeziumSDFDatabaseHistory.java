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

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DebeziumSDFDatabaseHistory extends AbstractDatabaseHistory {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumSDFDatabaseHistory.class);

    private List<byte[]> history;

    public DebeziumSDFDatabaseHistory() {
        this.history = new ArrayList<>();
    }

    @Override
    public void start() {
        super.start();
        LOG.debug("------------ STARTING THE DATABASE HISTORY! - trackers: {} - config: {}",
                KafkaSourceConsumerFn.restrictionTrackers, config.asMap());

        // TODO(pabloem): Figure out how to link these. For now, we'll just link directly.
        RestrictionTracker<DebeziumOffsetHolder, ?> tracker = KafkaSourceConsumerFn.restrictionTrackers.get(
                // JUST GETTING THE FIRST KEY. This will not work in the future.
                KafkaSourceConsumerFn.restrictionTrackers.keySet().iterator().next());
        this.history = (List<byte[]>) tracker.currentRestriction().history;
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        LOG.debug("------------- Adding history! {}", record);

        history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
        LOG.debug("------------- Trying to recover!");

        try {
            for (byte[] record : history) {
                Document doc = DocumentReader.defaultReader().read(record);
                consumer.accept(new HistoryRecord(doc));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() {
        return history != null && !history.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return history != null && !history.isEmpty();
    }
}
