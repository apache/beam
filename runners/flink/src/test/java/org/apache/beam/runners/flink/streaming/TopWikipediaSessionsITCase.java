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

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * Session window test.
 */
public class TopWikipediaSessionsITCase extends StreamingProgramTestBase implements Serializable {
  protected String resultPath;

  public TopWikipediaSessionsITCase(){
  }

  static final String[] EXPECTED_RESULT = new String[] {
      "user: user1 value:3",
      "user: user1 value:1",
      "user: user2 value:4",
      "user: user2 value:6",
      "user: user3 value:7",
      "user: user3 value:2"
  };

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

    Long now = (System.currentTimeMillis() + 10000) / 1000;

    PCollection<KV<String, Long>> output =
      p.apply(Create.of(Arrays.asList(new TableRow().set("timestamp", now).set
          ("contributor_username", "user1"), new TableRow().set("timestamp", now + 10).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now).set
          ("contributor_username", "user1"), new TableRow().set("timestamp", now + 2).set
          ("contributor_username", "user1"), new TableRow().set("timestamp", now).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 1).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 5).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 7).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 8).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 200).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 230).set
          ("contributor_username", "user1"), new TableRow().set("timestamp", now + 230).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 240).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now + 245).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 235).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 236).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 237).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 238).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 239).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 240).set
          ("contributor_username", "user3"), new TableRow().set("timestamp", now + 241).set
          ("contributor_username", "user2"), new TableRow().set("timestamp", now)
          .set("contributor_username", "user3"))))



      .apply(ParDo.of(new DoFn<TableRow, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          TableRow row = c.element();
          long timestamp = (Integer) row.get("timestamp");
          String userName = (String) row.get("contributor_username");
          if (userName != null) {
            // Sets the timestamp field to be used in windowing.
            c.outputWithTimestamp(userName, new Instant(timestamp * 1000L));
          }
        }
      }))

      .apply(Window.<String>into(Sessions.withGapDuration(Duration.standardMinutes(1))))

      .apply(Count.<String>perElement());

    PCollection<String> format = output.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        KV<String, Long> el = c.element();
        String out = "user: " + el.getKey() + " value:" + el.getValue();
        c.output(out);
      }
    }));

    format.apply(TextIO.Write.to(resultPath));

    p.run();
  }
}
