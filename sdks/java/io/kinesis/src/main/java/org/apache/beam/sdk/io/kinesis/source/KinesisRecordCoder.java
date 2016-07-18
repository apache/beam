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

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.kinesis.client.response.KinesisRecord;

import com.amazonaws.services.kinesis.model.Record;
import org.joda.time.Instant;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

/**
 * Created by p.pastuszka on 07/04/16.
 */
public class KinesisRecordCoder extends StandardCoder<KinesisRecord> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final ByteArrayCoder byteArrayCoder = ByteArrayCoder.of();
    private static final InstantCoder instantCoder = InstantCoder.of();
    private static final VarLongCoder varLongCoder = VarLongCoder.of();

    public static KinesisRecordCoder of() {
        return new KinesisRecordCoder();
    }

    @Override
    public void encode(KinesisRecord value, OutputStream outStream, Context context) throws
            CoderException, IOException {
        Context nested = context.nested();
        byteArrayCoder.encode(value.getData().array(), outStream, nested);
        stringCoder.encode(value.getSequenceNumber(), outStream, nested);
        stringCoder.encode(value.getPartitionKey(), outStream, nested);
        instantCoder.encode(new Instant(value.getApproximateArrivalTimestamp()), outStream, nested);
        varLongCoder.encode(value.getSubSequenceNumber(), outStream, nested);
        instantCoder.encode(new Instant(value.getReadTime()), outStream, nested);
        stringCoder.encode(value.getStreamName(), outStream, nested);
        stringCoder.encode(value.getShardId(), outStream, nested);
    }

    @Override
    public KinesisRecord decode(InputStream inStream, Context context) throws CoderException, IOException {
        Context nested = context.nested();
        ByteBuffer data = ByteBuffer.wrap(byteArrayCoder.decode(inStream, nested));
        String sequenceNumber = stringCoder.decode(inStream, nested);
        String partitionKey = stringCoder.decode(inStream, nested);
        Date approximateArrivalTimestamp = instantCoder.decode(inStream, nested).toDate();
        long subSequenceNumber = varLongCoder.decode(inStream, nested);
        Date readTimestamp = instantCoder.decode(inStream, nested).toDate();
        String streamName = stringCoder.decode(inStream, nested);
        String shardId = stringCoder.decode(inStream, nested);
        Record record = new Record().
                withData(data).
                withSequenceNumber(sequenceNumber).
                withPartitionKey(partitionKey).
                withApproximateArrivalTimestamp(approximateArrivalTimestamp);
        return new KinesisRecord(record, subSequenceNumber, readTimestamp, streamName, shardId);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        stringCoder.verifyDeterministic();
        byteArrayCoder.verifyDeterministic();
        instantCoder.verifyDeterministic();
        varLongCoder.verifyDeterministic();
    }
}
