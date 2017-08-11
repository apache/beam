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
package org.apache.beam.integration.nexmark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query 0: Pass events through unchanged. However, force them to do a round trip through
 * serialization so that we measure the impact of the choice of coders.
 */
public class Query0 extends NexmarkQuery {
  public Query0(NexmarkConfiguration configuration) {
    super(configuration, "Query0");
  }

  private PCollection<Event> applyTyped(PCollection<Event> events) {
    final Coder<Event> coder = events.getCoder();
    return events
        // Force round trip through coder.
        .apply(name + ".Serialize",
            ParDo.of(new DoFn<Event, Event>() {
                  private final Aggregator<Long, Long> bytes =
                      createAggregator("bytes", Sum.ofLongs());

                  @ProcessElement
                  public void processElement(ProcessContext c) throws CoderException, IOException {
                    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                    coder.encode(c.element(), outStream, Coder.Context.OUTER);
                    byte[] byteArray = outStream.toByteArray();
                    bytes.addValue((long) byteArray.length);
                    ByteArrayInputStream inStream = new ByteArrayInputStream(byteArray);
                    Event event = coder.decode(inStream, Coder.Context.OUTER);
                    c.output(event);
                  }
                }));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
