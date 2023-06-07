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
package org.apache.beam.sdk.io.components.deadletterqueue;

import org.apache.beam.sdk.io.components.deadletterqueue.sinks.ThrowingSink;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DLQRouterTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();


  @Test
  public void testExceptionWithInvalidConfiguration(){
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("DLQ Router only supports PCollectionTuples split between two message groupings");

    TupleTag<String> tag1 = new TupleTag<>();
    TupleTag<String> tag2 = new TupleTag<>();
    TupleTag<String> tag3 = new TupleTag<>();
    PCollectionTuple tuple = PCollectionTuple.of(tag1, p.apply(Create.<String>of("elem1")))
        .and(tag2, p.apply(Create.<String>of("elem2")))
        .and(tag3, p.apply(Create.<String>of("elem1")));
    tuple.apply(new DLQRouter<>(tag1, tag2, new ThrowingSink<>()));

    p.run();

  }

  @Test
  public void testExpectCorrectRouting(){
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("elem2");

    TupleTag<String> tag1 = new TupleTag<>();
    TupleTag<String> tag2 = new TupleTag<>();

    PCollectionTuple tuple = PCollectionTuple.of(tag1, p.apply("create elem1", Create.<String>of("elem1")))
        .and(tag2, p.apply("create elem2", Create.<String>of("elem2")));

    PCollection<String> expectedElement = tuple.apply(new DLQRouter<>(tag1, tag2, new ThrowingSink<>()));

    PAssert.thatSingleton(expectedElement).isEqualTo("elem1");

    p.run();
  }


}
