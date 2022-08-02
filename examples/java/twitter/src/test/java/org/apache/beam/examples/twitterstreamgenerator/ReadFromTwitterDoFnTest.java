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
package org.apache.beam.examples.twitterstreamgenerator;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import twitter4j.Status;

@RunWith(JUnit4.class)
public class ReadFromTwitterDoFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final ExpectedException expectedException = ExpectedException.none();
  @Mock TwitterConnection twitterConnection1;
  @Mock TwitterConnection twitterConnection2;
  @Mock Status status1;
  @Mock Status status2;
  @Mock Status status3;
  @Mock Status status4;
  @Mock Status status5;
  LinkedBlockingQueue<Status> queue1 = new LinkedBlockingQueue<>();
  LinkedBlockingQueue<Status> queue2 = new LinkedBlockingQueue<>();

  @Before
  public void setUp() throws JsonProcessingException {
    MockitoAnnotations.initMocks(this);
    when(status1.getText()).thenReturn("Breaking News1");
    when(status1.getCreatedAt()).thenReturn(new Date());
    when(status2.getText()).thenReturn("Breaking News2");
    when(status2.getCreatedAt()).thenReturn(new Date());
    when(status3.getText()).thenReturn("Breaking News3");
    when(status3.getCreatedAt()).thenReturn(new Date());
    when(status4.getText()).thenReturn("Breaking News4");
    when(status4.getCreatedAt()).thenReturn(new Date());
    when(status5.getText()).thenReturn("Breaking News5");
    when(status5.getCreatedAt()).thenReturn(new Date());
    queue1.offer(status1);
    queue1.offer(status2);
    queue1.offer(status3);
    queue2.offer(status4);
    queue2.offer(status5);
  }

  @Test
  public void testTwitterRead() {
    TwitterConfig twitterConfig = new TwitterConfig.Builder().setTweetsCount(3L).build();
    TwitterConnection.INSTANCE_MAP.put(twitterConfig, twitterConnection1);
    when(twitterConnection1.getQueue()).thenReturn(queue1);
    PCollection<String> result =
        pipeline
            .apply("Create Twitter Connection Configuration", Create.of(twitterConfig))
            .apply(ParDo.of(new ReadFromTwitterDoFn()));
    PAssert.that(result)
        .satisfies(
            pcollection -> {
              List<String> output = new ArrayList<>();
              pcollection.forEach(output::add);
              String[] expected = {"Breaking News1", "Breaking News2", "Breaking News3"};
              String[] actual = new String[output.size()];
              IntStream.range(0, output.size()).forEach((i) -> actual[i] = output.get(i));
              assertArrayEquals("Mismatch found in output", actual, expected);
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testMultipleTwitterConfigs() {
    TwitterConfig twitterConfig1 = new TwitterConfig.Builder().setTweetsCount(3L).build();
    TwitterConfig twitterConfig2 = new TwitterConfig.Builder().setTweetsCount(2L).build();
    TwitterConnection.INSTANCE_MAP.put(twitterConfig1, twitterConnection1);
    TwitterConnection.INSTANCE_MAP.put(twitterConfig2, twitterConnection2);
    when(twitterConnection1.getQueue()).thenReturn(queue1);
    when(twitterConnection2.getQueue()).thenReturn(queue2);
    PCollection<String> result =
        pipeline
            .apply(
                "Create Twitter Connection Configuration",
                Create.of(twitterConfig1, twitterConfig2))
            .apply(ParDo.of(new ReadFromTwitterDoFn()));
    PAssert.that(result)
        .satisfies(
            pcollection -> {
              List<String> output = new ArrayList<>();
              pcollection.forEach(output::add);
              String[] expected = {
                "Breaking News1",
                "Breaking News2",
                "Breaking News3",
                "Breaking News4",
                "Breaking News5"
              };
              String[] actual = new String[output.size()];
              Collections.sort(output);
              IntStream.range(0, output.size()).forEach((i) -> actual[i] = output.get(i));
              assertArrayEquals("Mismatch found in output", actual, expected);
              return null;
            });
    pipeline.run();
  }
}
