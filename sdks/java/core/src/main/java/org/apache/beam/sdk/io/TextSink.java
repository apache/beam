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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * Implementation detail of {@link TextIO.Write}.
 *
 * <p>A {@link FileBasedSink} for text files. Produces text files with the newline separator {@code
 * '\n'} represented in {@code UTF-8} format as the record separator. Each record (including the
 * last) is terminated.
 */
class TextSink extends FileBasedSink<String> {
  @Nullable private final String header;
  @Nullable private final String footer;

  TextSink(
      ValueProvider<ResourceId> baseOutputFilename,
      FilenamePolicy filenamePolicy,
      @Nullable String header,
      @Nullable String footer,
      WritableByteChannelFactory writableByteChannelFactory) {
    super(baseOutputFilename, filenamePolicy, writableByteChannelFactory);
    this.header = header;
    this.footer = footer;
  }
  @Override
  public FileBasedWriteOperation<String> createWriteOperation() {
    return new TextWriteOperation(this, header, footer);
  }

  /** A {@link FileBasedWriteOperation FileBasedWriteOperation} for text files. */
  private static class TextWriteOperation extends FileBasedWriteOperation<String> {
    @Nullable private final String header;
    @Nullable private final String footer;

    private TextWriteOperation(TextSink sink, @Nullable String header, @Nullable String footer) {
      super(sink);
      this.header = header;
      this.footer = footer;
    }

    @Override
    public FileBasedWriter<String> createWriter() throws Exception {
      return new TextWriter(this, header, footer);
    }
  }

  /** A {@link FileBasedWriter FileBasedWriter} for text files. */
  private static class TextWriter extends FileBasedWriter<String> {
    private static final String NEWLINE = "\n";
    @Nullable private final String header;
    @Nullable private final String footer;
    private OutputStreamWriter out;

    public TextWriter(
        FileBasedWriteOperation<String> writeOperation,
        @Nullable String header,
        @Nullable String footer) {
      super(writeOperation, MimeTypes.TEXT);
      this.header = header;
      this.footer = footer;
    }

    /** Writes {@code value} followed by a newline character if {@code value} is not null. */
    private void writeIfNotNull(@Nullable String value) throws IOException {
      if (value != null) {
        writeLine(value);
      }
    }

    /** Writes {@code value} followed by newline character. */
    private void writeLine(String value) throws IOException {
      out.write(value);
      out.write(NEWLINE);
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      out = new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8);
    }

    @Override
    protected void writeHeader() throws Exception {
      writeIfNotNull(header);
    }

    @Override
    public void write(String value) throws Exception {
      writeLine(value);
    }

    @Override
    protected void writeFooter() throws Exception {
      writeIfNotNull(footer);
    }

    @Override
    protected void finishWrite() throws Exception {
      out.flush();
    }
  }
}
