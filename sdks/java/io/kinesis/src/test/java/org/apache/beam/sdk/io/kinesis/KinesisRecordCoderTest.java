package org.apache.beam.sdk.io.kinesis;

import org.apache.beam.sdk.testing.CoderProperties;

import org.joda.time.Instant;
import org.junit.Test;
import java.nio.ByteBuffer;

/**
 * Created by p.pastuszka on 20.07.2016.
 */
public class KinesisRecordCoderTest {
    @Test
    public void encodingAndDecodingWorks() throws Exception {
        KinesisRecord record = new KinesisRecord(
                ByteBuffer.wrap("data".getBytes()),
                "sequence",
                128L,
                "partition",
                Instant.now(),
                Instant.now(),
                "stream",
                "shard"
        );
        CoderProperties.coderDecodeEncodeEqual(
                new KinesisRecordCoder(), record
        );
    }
}
