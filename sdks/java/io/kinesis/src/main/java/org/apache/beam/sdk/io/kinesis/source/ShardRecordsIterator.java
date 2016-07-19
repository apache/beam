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
package org.apache.beam.sdk.io.kinesis.source;

import org.apache.beam.sdk.io.kinesis.client.SimplifiedKinesisClient;
import org.apache.beam.sdk.io.kinesis.client.TransientKinesisException;
import org.apache.beam.sdk.io.kinesis.client.response.GetKinesisRecordsResult;
import org.apache.beam.sdk.io.kinesis.client.response.KinesisRecord;
import org.apache.beam.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Queues.newArrayDeque;


import com.google.common.base.CustomOptional;
import com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Deque;

/***
 * Iterates over records in a single shard.
 * Under the hood records are retrieved from Kinesis in batches and stored in the in-memory queue.
 * Then the caller of {@link ShardRecordsIterator#next()} can read from queue one by one.
 */
public class ShardRecordsIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final SimplifiedKinesisClient kinesis;
    private final RecordFilter filter;
    private ShardCheckpoint checkpoint;
    private String shardIterator;
    private Deque<KinesisRecord> data = newArrayDeque();

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient) throws
            TransientKinesisException {
        this(initialCheckpoint, simplifiedKinesisClient, new RecordFilter());
    }

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient,
                                RecordFilter filter) throws
            TransientKinesisException {
        checkNotNull(initialCheckpoint);
        checkNotNull(simplifiedKinesisClient);

        this.checkpoint = initialCheckpoint;
        this.filter = filter;
        this.kinesis = simplifiedKinesisClient;
        shardIterator = checkpoint.getShardIterator(kinesis);
    }

    /***
     * Returns record if there's any present.
     * Returns absent() if there are no new records at this time in the shard.
     */
    public Optional<KinesisRecord> next() throws TransientKinesisException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return CustomOptional.absent();
        } else {
            KinesisRecord record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record);
            return CustomOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws TransientKinesisException {
        if (data.isEmpty()) {
            GetKinesisRecordsResult response;
            try {
                response = kinesis.getRecords(shardIterator, checkpoint.getStreamName(),
                                              checkpoint.getShardId());
            } catch (ExpiredIteratorException e) {
                LOG.info("Refreshing expired iterator", e);
                shardIterator = checkpoint.getShardIterator(kinesis);
                response = kinesis.getRecords(shardIterator, checkpoint.getStreamName(),
                                              checkpoint.getShardId());
            }
            LOG.debug("Fetched {} new records", response.getRecords().size());
            shardIterator = response.getNextShardIterator();
            data.addAll(filter.apply(response.getRecords(), checkpoint));
        }
    }

    public ShardCheckpoint getCheckpoint() {
        return checkpoint;
    }


}
