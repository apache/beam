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
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * A simple {@link FileBasedSink} that writes {@link String} values as lines with header and footer.
 */
class SimpleSink<DestinationT> extends FileBasedSink<String, DestinationT, String> {
  public SimpleSink(
      ResourceId tempDirectory,
      DynamicDestinations<String, DestinationT, String> dynamicDestinations,
      WritableByteChannelFactory writableByteChannelFactory) {
    super(StaticValueProvider.of(tempDirectory), dynamicDestinations, writableByteChannelFactory);
  }

  public SimpleSink(
      ResourceId tempDirectory,
      DynamicDestinations<String, DestinationT, String> dynamicDestinations,
      Compression compression) {
    super(StaticValueProvider.of(tempDirectory), dynamicDestinations, compression);
  }

  public static SimpleSink<Void> makeSimpleSink(
      ResourceId tempDirectory, FilenamePolicy filenamePolicy) {
    return new SimpleSink<>(
        tempDirectory, DynamicFileDestinations.constant(filenamePolicy), Compression.UNCOMPRESSED);
  }

  public static SimpleSink<Void> makeSimpleSink(
      ResourceId baseDirectory,
      String prefix,
      String shardTemplate,
      String suffix,
      WritableByteChannelFactory writableByteChannelFactory) {
    DynamicDestinations<String, Void, String> dynamicDestinations =
        DynamicFileDestinations.constant(
            DefaultFilenamePolicy.fromParams(
                new Params()
                    .withBaseFilename(
                        baseDirectory.resolve(prefix, StandardResolveOptions.RESOLVE_FILE))
                    .withShardTemplate(shardTemplate)
                    .withSuffix(suffix)));
    return new SimpleSink<>(baseDirectory, dynamicDestinations, writableByteChannelFactory);
  }

  public static SimpleSink<Void> makeSimpleSink(
      ResourceId baseDirectory,
      String prefix,
      String shardTemplate,
      String suffix,
      Compression compression) {
    return makeSimpleSink(
        baseDirectory,
        prefix,
        shardTemplate,
        suffix,
        FileBasedSink.CompressionType.fromCanonical(compression));
  }

  @Override
  public SimpleWriteOperation<DestinationT> createWriteOperation() {
    return new SimpleWriteOperation<>(this);
  }

  static final class SimpleWriteOperation<DestinationT>
      extends WriteOperation<DestinationT, String> {
    public SimpleWriteOperation(SimpleSink<DestinationT> sink, ResourceId tempOutputDirectory) {
      super(sink, tempOutputDirectory);
    }

    public SimpleWriteOperation(SimpleSink<DestinationT> sink) {
      super(sink);
    }

    @Override
    public SimpleWriter<DestinationT> createWriter() {
      return new SimpleWriter<>(this);
    }

    public ResourceId getTempDirectory() {
      return tempDirectory.get();
    }
  }

  static final class SimpleWriter<DestinationT> extends Writer<DestinationT, String> {
    static final String HEADER = "header";
    static final String FOOTER = "footer";

    private WritableByteChannel channel;

    public SimpleWriter(SimpleWriteOperation<DestinationT> writeOperation) {
      super(writeOperation, MimeTypes.TEXT);
    }

    private static ByteBuffer wrap(String value) {
      return ByteBuffer.wrap((value + "\n").getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) {
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
