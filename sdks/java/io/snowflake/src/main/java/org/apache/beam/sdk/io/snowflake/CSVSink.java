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
package org.apache.beam.sdk.io.snowflake;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

/** Implementation of {@link org.apache.beam.sdk.io.FileIO.Sink} for writing CSV. */
public class CSVSink implements FileIO.Sink<String> {
  private String header;
  private transient PrintWriter writer;

  public CSVSink(List<String> colNames) {
    if (!colNames.isEmpty()) {
      this.header = Joiner.on(",").join(colNames);
    }
  }

  public CSVSink() {
    this.header = null;
  }

  @Override
  public void open(WritableByteChannel channel) throws IOException {
    writer =
        new PrintWriter(
            new BufferedWriter(
                new OutputStreamWriter(
                    Channels.newOutputStream(channel), Charset.defaultCharset())));
    if (this.header != null) {
      writer.println(header);
    }
  }

  @Override
  public void write(String element) throws IOException {
    writer.println(element);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }
}
