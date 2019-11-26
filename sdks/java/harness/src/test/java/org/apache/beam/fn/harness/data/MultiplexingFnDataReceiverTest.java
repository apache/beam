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
package org.apache.beam.fn.harness.data;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MultiplexingFnDataReceiver}. */
@RunWith(JUnit4.class)
public class MultiplexingFnDataReceiverTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void singleConsumer() throws Exception {
    List<String> consumer = new ArrayList<>();
    FnDataReceiver<String> multiplexer =
        MultiplexingFnDataReceiver.forConsumers(
            ImmutableList.<FnDataReceiver<String>>of(consumer::add));

    multiplexer.accept("foo");
    multiplexer.accept("bar");

    assertThat(consumer, contains("foo", "bar"));
  }

  @Test
  public void singleConsumerException() throws Exception {
    String message = "my_exception";
    FnDataReceiver<Integer> multiplexer =
        MultiplexingFnDataReceiver.forConsumers(
            ImmutableList.<FnDataReceiver<Integer>>of(
                (Integer i) -> {
                  if (i > 1) {
                    throw new Exception(message);
                  }
                }));

    multiplexer.accept(0);
    multiplexer.accept(1);
    thrown.expectMessage(message);
    thrown.expect(Exception.class);
    multiplexer.accept(2);
  }

  @Test
  public void multipleConsumers() throws Exception {
    List<String> consumer = new ArrayList<>();
    Set<String> otherConsumer = new HashSet<>();
    FnDataReceiver<String> multiplexer =
        MultiplexingFnDataReceiver.forConsumers(
            ImmutableList.<FnDataReceiver<String>>of(consumer::add, otherConsumer::add));

    multiplexer.accept("foo");
    multiplexer.accept("bar");
    multiplexer.accept("foo");

    assertThat(consumer, contains("foo", "bar", "foo"));
    assertThat(otherConsumer, containsInAnyOrder("foo", "bar"));
  }

  @Test
  public void multipleConsumersException() throws Exception {
    String message = "my_exception";
    List<Integer> consumer = new ArrayList<>();
    FnDataReceiver<Integer> multiplexer =
        MultiplexingFnDataReceiver.forConsumers(
            ImmutableList.<FnDataReceiver<Integer>>of(
                consumer::add,
                (Integer i) -> {
                  if (i > 1) {
                    throw new Exception(message);
                  }
                }));

    multiplexer.accept(0);
    multiplexer.accept(1);
    assertThat(consumer, containsInAnyOrder(0, 1));

    thrown.expectMessage(message);
    thrown.expect(Exception.class);
    multiplexer.accept(2);
  }
}
