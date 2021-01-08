package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class DebeziumOffsetTrackerTest implements Serializable {

    @Test
    public void testRestrictByNumberOfRecords() throws IOException {
        int maxNumRecords = 10;
        Map<String, Object> position = new HashMap<>();
        KafkaSourceConsumerFn<String> kafkaSourceConsumerFn = new KafkaSourceConsumerFn<String>(MySqlConnector.class,
                new SourceRecordJson.SourceRecordJsonMapper());
        DebeziumOffsetHolder restriction = kafkaSourceConsumerFn.getInitialRestriction(new HashMap<>());
        DebeziumOffsetTracker tracker = new DebeziumOffsetTracker(restriction);
        for (int records=0; records<maxNumRecords; records++) {
            assertTrue("OffsetTracker should continue",tracker.tryClaim(position));
        }
        assertFalse("OffsetTracker should stop", tracker.tryClaim(position));
    }

    @Test
    public void testRestrictByAmountOfTime() throws IOException, InterruptedException {
        long MILLIS = 60 * 1000;
        long minutesToRun = 1;
        Map<String, Object> position = new HashMap<>();
        KafkaSourceConsumerFn<String> kafkaSourceConsumerFn = new KafkaSourceConsumerFn<String>(MySqlConnector.class,
                new SourceRecordJson.SourceRecordJsonMapper(), minutesToRun);
        DebeziumOffsetHolder restriction = kafkaSourceConsumerFn.getInitialRestriction(new HashMap<>());
        DebeziumOffsetTracker tracker = new DebeziumOffsetTracker(restriction);
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0;
        long tolerance = 50;
        while (elapsedTime < (minutesToRun * MILLIS)) {
            assertTrue("OffsetTracker should continue: "+elapsedTime+" / "+minutesToRun * MILLIS,
                    tracker.tryClaim(position));
            elapsedTime = System.currentTimeMillis() - startTime + tolerance;
        }
        Thread.sleep(1000);
        assertFalse("OffsetTracker should have stopped: "+elapsedTime+" / "+minutesToRun * MILLIS,
                tracker.tryClaim(position));
    }
}
