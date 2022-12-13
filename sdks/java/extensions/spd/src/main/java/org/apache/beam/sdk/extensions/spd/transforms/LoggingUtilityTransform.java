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
package org.apache.beam.sdk.extensions.spd.transforms;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingUtilityTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingUtilityTransform.class);

  @Nullable private String prefix = "";

  public void setPrefix(@Nullable String prefix) {
    this.prefix = prefix;
  }

  public @Nullable String getPrefix() {
    return prefix;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(
            ParDo.of(
                new DoFn<Row, Row>() {
                  @ProcessElement
                  public void processElement(
                      @Element Row row, OutputReceiver<Row> out, ProcessContext c) {
                    String logLine = (prefix == null ? "" : prefix) + row;
                    LOG.info(logLine);
                    out.output(row);
                  }
                }))
        .setCoder(input.getCoder())
        .setRowSchema(input.getSchema());
  }
}
