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

import com.google.common.base.Joiner;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
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
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Test for GroupByNullKey.
 */
public class GroupByNullKeyTest extends StreamingProgramTestBase implements Serializable {

  protected String resultDir;
  protected String resultPath;

  static final String[] EXPECTED_RESULT = new String[] {
      "k: null v: user1 user1 user1 user2 user2 user2 user2 user3"
  };

  public GroupByNullKeyTest() {
  }

  @Override
  protected void preSubmit() throws Exception {
    // Beam Write will add shard suffix to fileName, see ShardNameTemplate.
    // So tempFile need have a parent to compare.
    File resultParent = createAndRegisterTempFile("result");
    resultDir = resultParent.toURI().toString();
    resultPath = new File(resultParent, "file.txt").getAbsolutePath();
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultDir);
  }

  /**
   * DoFn extracting user and timestamp.
   */
  private static class ExtractUserAndTimestamp extends DoFn<KV<Integer, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, String> record = c.element();
      int timestamp = record.getKey();
      String userName = record.getValue();
      if (userName != null) {
        // Sets the implicit timestamp field to be used in windowing.
        c.outputWithTimestamp(userName, new Instant(timestamp));
      }
    }
  }

  @Override
  protected void testProgram() throws Exception {

    Pipeline p = FlinkTestPipeline.createForStreaming();

    PCollection<String> output =
        p.apply(
                Create.of(
                    Arrays.asList(
                        KV.of(0, "user1"),
                        KV.of(1, "user1"),
                        KV.of(2, "user1"),
                        KV.of(10, "user2"),
                        KV.of(1, "user2"),
                        KV.of(15000, "user2"),
                        KV.of(12000, "user2"),
                        KV.of(25000, "user3"))))
            .apply(ParDo.of(new ExtractUserAndTimestamp()))
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardHours(1)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(
                ParDo.of(
                    new DoFn<String, KV<Void, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        String elem = c.element();
                        c.output(KV.of(null, elem));
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<Void, Iterable<String>>, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        KV<Void, Iterable<String>> elem = c.element();
                        StringBuilder str = new StringBuilder();
                        str.append("k: " + elem.getKey() + " v:");
                        for (String v : elem.getValue()) {
                          str.append(" " + v);
                        }
                        c.output(str.toString());
                      }
                    }));
    output.apply(TextIO.write().to(resultPath));
    p.run();
  }
}
