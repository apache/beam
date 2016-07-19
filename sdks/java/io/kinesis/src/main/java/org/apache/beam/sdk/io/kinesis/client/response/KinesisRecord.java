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
package org.apache.beam.sdk.io.kinesis.client.response;

import com.google.common.base.Charsets;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;
import org.apache.commons.lang.builder.EqualsBuilder;
import java.util.Date;

/**
 * {@link UserRecord} enhanced with utility methods.
 */
public class KinesisRecord extends UserRecord {
    private Date readTime = new Date();
    private String streamName;
    private String shardId;

    public KinesisRecord(UserRecord record, String streamName, String shardId) {
        super(record.isAggregated(),
                record,
                record.getSubSequenceNumber(),
                record.getExplicitHashKey());
        this.streamName = streamName;
        this.shardId = shardId;
    }

    public KinesisRecord(Record record, long subSequenceNumber, Date readTime,
                         String streamName, String shardId) {
        super(false, record, subSequenceNumber, null);
        this.readTime = new Date(readTime.getTime());
        this.streamName = streamName;
        this.shardId = shardId;
    }

    public ExtendedSequenceNumber getExtendedSequenceNumber() {
        return new ExtendedSequenceNumber(getSequenceNumber(), getSubSequenceNumber());
    }

    /***
     * @return unique id of the record based on its position in the stream
     */
    public byte[] getUniqueId() {
        return getExtendedSequenceNumber().toString().getBytes(Charsets.UTF_8);
    }

    public Date getReadTime() {
        return new Date(readTime.getTime());
    }

    public String getStreamName() {
        return streamName;
    }

    public String getShardId() {
        return shardId;
    }

    public byte[] getDataAsBytes() {
        return getData().array();
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return reflectionHashCode(this);
    }
}
