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
package org.apache.beam.io.debezium;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.debezium.connector.mysql.MySqlConnector;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OffsetTrackerTest implements Serializable {
  @Test
  public void testRestrictByNumberOfRecords() throws IOException {
    Integer maxNumRecords = 10;
    Map<String, Object> position = new HashMap<>();
    KafkaSourceConsumerFn<String> kafkaSourceConsumerFn =
        new KafkaSourceConsumerFn<String>(
            MySqlConnector.class, new SourceRecordJson.SourceRecordJsonMapper(), maxNumRecords);
    KafkaSourceConsumerFn.OffsetHolder restriction =
        kafkaSourceConsumerFn.getInitialRestriction(new HashMap<>());
    KafkaSourceConsumerFn.OffsetTracker tracker =
        new KafkaSourceConsumerFn.OffsetTracker(restriction);

    for (int records = 0; records < maxNumRecords; records++) {
      assertTrue("OffsetTracker should continue", tracker.tryClaim(position));
    }
    assertFalse("OffsetTracker should stop", tracker.tryClaim(position));
  }

  @Test
  public void testRestrictByAmountOfTime() throws IOException, InterruptedException {
    Map<String, Object> position = new HashMap<>();
    KafkaSourceConsumerFn<String> kafkaSourceConsumerFn =
        new KafkaSourceConsumerFn<String>(
            MySqlConnector.class,
            new SourceRecordJson.SourceRecordJsonMapper(),
            100000,
            500L); // Run for 500 ms
    KafkaSourceConsumerFn.OffsetHolder restriction =
        kafkaSourceConsumerFn.getInitialRestriction(new HashMap<>());
    KafkaSourceConsumerFn.OffsetTracker tracker =
        new KafkaSourceConsumerFn.OffsetTracker(restriction);

    assertTrue(tracker.tryClaim(position));

    Thread.sleep(1000); // Sleep for a whole 2 seconds

    assertFalse(tracker.tryClaim(position));
  }
}
