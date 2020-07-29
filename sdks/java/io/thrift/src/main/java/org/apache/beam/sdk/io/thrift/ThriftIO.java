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
package org.apache.beam.sdk.io.thrift;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.AutoExpandingBufferReadTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing files containing Thrift encoded data.
 *
 * <h3>Reading Thrift Files</h3>
 *
 * <p>For reading each file in a {@link PCollection} of {@link FileIO.ReadableFile}, use the {@link
 * ThriftIO#readFiles(Class)} transform.
 *
 * <p>For example:
 *
 * <pre>{@code
 * PCollection<FileIO.ReadableFile> files = pipeline
 *   .apply(FileIO.match().filepattern(options.getInputFilepattern())
 *   .apply(FileIO.readMatches());
 *
 * PCollection<ExampleType> examples = files.apply(ThriftIO.readFiles(ExampleType.class).withProtocol(thriftProto);
 * }</pre>
 *
 * <h3>Writing Thrift Files</h3>
 *
 * <p>{@link ThriftIO.Sink} allows for a {@link PCollection} of {@link TBase} to be written to
 * Thrift files. It can be used with the general-purpose {@link FileIO} transforms with
 * FileIO.write/writeDynamic specifically.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // PCollection<ExampleType>
 *   .apply(FileIO
 *     .<ExampleType>write()
 *     .via(ThriftIO.sink(thriftProto))
 *     .to("destination/path");
 * }</pre>
 *
 * <p>This IO API is considered experimental and may break or receive backwards-incompatible changes
 * in future versions of the Apache Beam SDK.
 */
@Experimental(Kind.SOURCE_SINK)
public class ThriftIO {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftIO.class);

  /** Disable construction of utility class. */
  private ThriftIO() {}

  /**
   * Reads each file in a {@link PCollection} of {@link org.apache.beam.sdk.io.FileIO.ReadableFile},
   * which allows more flexible usage.
   */
  public static <T> ReadFiles<T> readFiles(Class<T> recordClass) {
    return new AutoValue_ThriftIO_ReadFiles.Builder<T>().setRecordClass(recordClass).build();
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Creates a {@link Sink} for use with {@link FileIO#write} and {@link FileIO#writeDynamic}. */
  public static <T extends TBase<?, ?>> Sink<T> sink(TProtocolFactory factory) {
    return new AutoValue_ThriftIO_Sink.Builder<T>().setProtocolFactory(factory).build();
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #readFiles}. */
  @AutoValue
  public abstract static class ReadFiles<T>
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {

    abstract Builder<T> toBuilder();

    abstract @Nullable Class<T> getRecordClass();

    abstract @Nullable TProtocolFactory getTProtocolFactory();

    /**
     * Specifies the {@link TProtocolFactory} to be used to decode Thrift objects.
     *
     * @param protocol {@link TProtocolFactory} used to decode Thrift objects.
     * @return ReadFiles object with protocol set.
     * @throws IllegalArgumentException if {@link TSimpleJSONProtocol} is passed as it is
     *     write-only.
     */
    public ReadFiles<T> withProtocol(TProtocolFactory protocol) {
      checkArgument(
          !(protocol instanceof TSimpleJSONProtocol.Factory),
          "TSimpleJSONProtocol is a write only protocol");
      return toBuilder().setTProtocolFactory(protocol).build();
    }

    @Override
    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
      checkNotNull(getRecordClass(), "Record class cannot be null");
      checkNotNull(getTProtocolFactory(), "Thrift protocol cannot be null");

      return input
          .apply(ParDo.of(new ReadFn<>(getRecordClass(), getTProtocolFactory())))
          .setCoder(ThriftCoder.of(getRecordClass(), getTProtocolFactory()));
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setRecordClass(Class<T> recordClass);

      abstract TProtocolFactory getTProtocolFactory();

      abstract Builder<T> setTProtocolFactory(TProtocolFactory tProtocol);

      abstract ReadFiles<T> build();
    }

    /**
     * Reads a {@link FileIO.ReadableFile} and outputs a {@link PCollection} of {@link
     * #getRecordClass()}.
     */
    static class ReadFn<T> extends DoFn<FileIO.ReadableFile, T> {

      final Class<T> tBaseType;
      final TProtocolFactory tProtocol;

      ReadFn(Class<T> tBaseType, TProtocolFactory tProtocol) {
        this.tBaseType = tBaseType;
        this.tProtocol = tProtocol;
      }

      @ProcessElement
      public void processElement(@Element FileIO.ReadableFile file, OutputReceiver<T> out) {
        try {
          InputStream inputStream = Channels.newInputStream(file.open());
          TIOStreamTransport streamTransport =
              new TIOStreamTransport(new BufferedInputStream(inputStream));
          AutoExpandingBufferReadTransport readTransport =
              new AutoExpandingBufferReadTransport(262_144_000);
          readTransport.fill(streamTransport, inputStream.available());
          TProtocol protocol = tProtocol.getProtocol(readTransport);
          while (protocol.getTransport().getBytesRemainingInBuffer() > 0) {
            TBase<?, ?> tb = (TBase<?, ?>) tBaseType.getDeclaredConstructor().newInstance();
            tb.read(protocol);
            out.output((T) tb);
          }
        } catch (Exception ioe) {
          String filename = file.getMetadata().resourceId().toString();
          LOG.error(String.format("Error in reading file: %1$s%n%2$s", filename, ioe));
          throw new RuntimeException(ioe);
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("Thrift class", getRecordClass().toString()).withLabel("Thrift class"));
      builder.add(
          DisplayData.item("Thrift Protocol", getTProtocolFactory().toString())
              .withLabel("Protocol Type"));
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #sink}. */
  @AutoValue
  public abstract static class Sink<T extends TBase<?, ?>> implements FileIO.Sink<T> {

    abstract Builder<T> toBuilder();

    abstract @Nullable TProtocolFactory getProtocolFactory();

    private transient ThriftWriter<T> writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.writer = new ThriftWriter<>(Channels.newOutputStream(channel), getProtocolFactory());
    }

    @Override
    public void write(T element) throws IOException {
      checkNotNull(writer, "Writer cannot be null");
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      writer.close();
    }

    @AutoValue.Builder
    abstract static class Builder<T extends TBase<?, ?>> {

      abstract Builder<T> setProtocolFactory(TProtocolFactory protocolFactory);

      abstract TProtocolFactory getProtocolFactory();

      abstract Sink<T> autoBuild();

      public Sink<T> build() {
        checkArgument(getProtocolFactory() != null, "TProtocol is required for sink.");
        return autoBuild();
      }
    }
  }

  /** Writer to write Thrift object to {@link OutputStream}. */
  protected static class ThriftWriter<T extends TBase<?, ?>> {
    private OutputStream stream;
    private TProtocolFactory protocolFactory;

    ThriftWriter(OutputStream stream, TProtocolFactory protocolFactory) {
      this.stream = stream;
      this.protocolFactory = protocolFactory;
    }

    public void write(T element) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));

      try {
        element.write(protocol);
      } catch (TException te) {
        LOG.error("Error in writing element to TProtocol: " + te);
        throw new RuntimeException(te);
      }
      this.stream.write(baos.toByteArray());
    }

    public void close() throws IOException {
      this.stream.flush();
      this.stream.close();
    }
  }
}
