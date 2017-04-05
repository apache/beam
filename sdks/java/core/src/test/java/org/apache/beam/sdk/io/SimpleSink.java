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
package org.apache.beam.sdk.io;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A simple FileBasedSink that writes String values as lines with header and footer lines.
 */
class SimpleSink extends FileBasedSink<String> {
  public SimpleSink(String baseOutputFilename, String extension) {
    super(baseOutputFilename, extension);
  }

  public SimpleSink(String baseOutputFilename, String extension,
                    WritableByteChannelFactory writableByteChannelFactory) {
    super(baseOutputFilename, extension, writableByteChannelFactory);
  }

  public SimpleSink(String baseOutputFilename, String extension, String fileNamingTemplate) {
    super(baseOutputFilename, extension, fileNamingTemplate);
  }

  @Override
  public SimpleWriteOperation createWriteOperation(PipelineOptions options) {
    return new SimpleWriteOperation(this);
  }

  static final class SimpleWriteOperation extends FileBasedWriteOperation<String> {
    public SimpleWriteOperation(SimpleSink sink, String tempOutputFilename) {
      super(sink, tempOutputFilename);
    }

    public SimpleWriteOperation(SimpleSink sink) {
      super(sink);
    }

    @Override
    public SimpleWriter createWriter(PipelineOptions options) throws Exception {
      return new SimpleWriter(this);
    }
  }

  static final class SimpleWriter extends FileBasedWriter<String> {
    static final String HEADER = "header";
    static final String FOOTER = "footer";

    private WritableByteChannel channel;

    public SimpleWriter(SimpleWriteOperation writeOperation) {
      super(writeOperation);
    }

    private static ByteBuffer wrap(String value) throws Exception {
      return ByteBuffer.wrap((value + "\n").getBytes("UTF-8"));
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      this.channel = channel;
    }

    @Override
    protected void writeHeader() throws Exception {
      channel.write(wrap(HEADER));
    }

    @Override
    protected void writeFooter() throws Exception {
      channel.write(wrap(FOOTER));
    }

    @Override
    public void write(String value) throws Exception {
      channel.write(wrap(value));
    }
  }
}
