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
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Config;
import org.apache.beam.sdk.io.DynamicDestinationHelpers.ConstantFilenamePolicy;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * A simple {@link FileBasedSink} that writes {@link String} values as lines with
 * header and footer.
 */
class SimpleSink extends FileBasedSink<String, Void> {
  private DynamicDestinations<String, Void> dynamicDestinations;

  public SimpleSink(ResourceId baseOutputDirectory, String prefix, String template, String suffix) {
    this(baseOutputDirectory, prefix, template, suffix, CompressionType.UNCOMPRESSED);
  }

  public SimpleSink(ResourceId baseOutputDirectory, String prefix, String template, String suffix,
                    WritableByteChannelFactory writableByteChannelFactory) {
    super(
        StaticValueProvider.of(baseOutputDirectory),
        writableByteChannelFactory);
    dynamicDestinations = new ConstantFilenamePolicy<>(
        DefaultFilenamePolicy.fromConfig(new Config(
            baseOutputDirectory.resolve(prefix, StandardResolveOptions.RESOLVE_FILE),
            template, suffix)));
  }

  public SimpleSink(ResourceId baseOutputDirectory, FilenamePolicy filenamePolicy) {
    super(StaticValueProvider.of(baseOutputDirectory), filenamePolicy);
  }

  @Override
  public SimpleWriteOperation createWriteOperation() {
    return new SimpleWriteOperation(this);
  }

  public DynamicDestinations<String, Void> getDynamicDestinations() {
    return dynamicDestinations;
  }

  static final class SimpleWriteOperation extends WriteOperation<String, Void> {
    public SimpleWriteOperation(SimpleSink sink,
                                ResourceId tempOutputDirectory) {
      super(sink, sink.dynamicDestinations, tempOutputDirectory);
    }

    public SimpleWriteOperation(SimpleSink sink) {
      super(sink, sink.dynamicDestinations);
    }

    @Override
    public SimpleWriter createWriter() throws Exception {
      return new SimpleWriter(this);
    }
  }

  static final class SimpleWriter extends Writer<String, Void> {
    static final String HEADER = "header";
    static final String FOOTER = "footer";

    private WritableByteChannel channel;

    public SimpleWriter(SimpleWriteOperation writeOperation) {
      super(writeOperation, MimeTypes.TEXT);
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
