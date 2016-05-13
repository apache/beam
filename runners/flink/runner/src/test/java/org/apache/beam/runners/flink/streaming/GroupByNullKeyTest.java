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
package org.apache.beam.runners.flink.streaming;

import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class GroupByNullKeyTest extends StreamingProgramTestBase implements Serializable {


  protected String resultPath;

  static final String[] EXPECTED_RESULT = new String[] {
      "k: null v: user1 user1 user1 user2 user2 user2 user2 user3"
  };

  public GroupByNullKeyTest(){
  }

  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
  }

  @Override
  protected void testProgram() throws Exception {

    Pipeline p = FlinkTestPipeline.createForStreaming();

    PCollection<String> output = p
        .apply(Create.timestamped(
            TimestampedValue.of("user1", new Instant(0)),
            TimestampedValue.of("user1", new Instant(1)),
            TimestampedValue.of("user1", new Instant(2)),
            TimestampedValue.of("user2", new Instant(10)),
            TimestampedValue.of("user2", new Instant(1)),
            TimestampedValue.of("user2", new Instant(15000)),
            TimestampedValue.of("user2", new Instant(12000)),
            TimestampedValue.of("user3", new Instant(25000))))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardHours(1)))
            .triggering(AfterWatermark.pastEndOfWindow())
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes())

        .apply(ParDo.of(new DoFn<String, KV<Void, String>>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            String elem = c.element();
            c.output(KV.<Void, String>of(null, elem));
          }
        }))
        .apply(GroupByKey.<Void, String>create())
        .apply(ParDo.of(new DoFn<KV<Void, Iterable<String>>, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            KV<Void, Iterable<String>> elem = c.element();
            ArrayList<String> strings = Lists.newArrayList(elem.getValue());
            Collections.sort(strings);
            StringBuilder str = new StringBuilder();
            str.append("k: " + elem.getKey() + " v:");
            for (String v : strings) {
              str.append(" " + v);
            }
            c.output(str.toString());
          }
        }));

    output.apply(TextIO.Write.to(resultPath));

    p.run();
  }
}
