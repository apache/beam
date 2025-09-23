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
package org.apache.beam.sdk.io.pulsar;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(https://github.com/apache/beam/issues/31078) exceptions are currently suppressed
@SuppressWarnings("Slf4jDoNotLogMessageOfExceptionExplicitly")
@RunWith(JUnit4.class)
public class PulsarIOTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static final Logger LOG = LoggerFactory.getLogger(PulsarIOTest.class);

  private static final String TEST_TOPIC = "TEST_TOPIC";
  // In order to pin fake readers having same set of messages
  private static final long START_TIMESTAMP = Instant.now().toEpochMilli();

  /** Create a fake client. */
  static PulsarClient newFakeClient() {
    return new FakePulsarClient(new FakePulsarReader(TEST_TOPIC, 10, START_TIMESTAMP));
  }

  @Test
  public void testRead() {

    PCollection<Integer> pcoll =
        pipeline
            .apply(
                PulsarIO.read()
                    .withTopic(TEST_TOPIC)
                    .withPulsarClient((ignored -> newFakeClient())))
            .apply(
                MapElements.into(TypeDescriptor.of(Integer.class))
                    .via(m -> (int) m.getMessageId()[1]));
    PAssert.that(pcoll)
        .satisfies(
            iterable -> {
              List<Integer> result = new ArrayList<Integer>();
              iterable.forEach(result::add);
              Assert.assertArrayEquals(
                  result.toArray(), new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testExpandReadFailUnserializableType() {
    pipeline.apply(
        PulsarIO.read(t -> t).withTopic(TEST_TOPIC).withPulsarClient((ignored -> newFakeClient())));
    IllegalStateException exception =
        Assert.assertThrows(IllegalStateException.class, pipeline::run);
    String errorMsg = exception.getMessage();
    Assert.assertTrue(
        "Actual message: " + errorMsg,
        exception.getMessage().contains("Unable to return a default Coder for PulsarIO.Read"));
    pipeline.enableAbandonedNodeEnforcement(false);
  }
}
