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
package org.apache.beam.sdk.io.xml;

import com.google.common.annotations.VisibleForTesting;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MimeTypes;

/** Implementation of {@link XmlIO#write}. */
class XmlSink<T> extends FileBasedSink<T, Void, T> {
  private static final String XML_EXTENSION = ".xml";

  private final XmlIO.Write<T> spec;

  private static <T> DefaultFilenamePolicy makeFilenamePolicy(XmlIO.Write<T> spec) {
    return DefaultFilenamePolicy.fromStandardParameters(
        spec.getFilenamePrefix(), ShardNameTemplate.INDEX_OF_MAX, XML_EXTENSION, false);
  }

  XmlSink(XmlIO.Write<T> spec) {
    super(spec.getFilenamePrefix(), DynamicFileDestinations.<T>constant(makeFilenamePolicy(spec)));
    this.spec = spec;
  }

  /**
   * Creates an {@link XmlWriteOperation}.
   */
  @Override
  public XmlWriteOperation<T> createWriteOperation() {
    return new XmlWriteOperation<>(this);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    spec.populateDisplayData(builder);
  }

  void populateFileBasedDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  /** {@link WriteOperation} for XML {@link FileBasedSink}s. */
  protected static final class XmlWriteOperation<T> extends WriteOperation<Void, T> {
    public XmlWriteOperation(XmlSink<T> sink) {
      super(sink);
    }

    /**
     * Creates a {@link XmlWriter} with a marshaller for the type it will write.
     */
    @Override
    public XmlWriter<T> createWriter() throws Exception {
      JAXBContext context;
      Marshaller marshaller;
      context = JAXBContext.newInstance(getSink().spec.getRecordClass());
      marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_ENCODING, getSink().spec.getCharset());
      return new XmlWriter<>(this, marshaller);
    }

    /**
     * Return the XmlSink.Bound for this write operation.
     */
    @Override
    public XmlSink<T> getSink() {
      return (XmlSink<T>) super.getSink();
    }

    @VisibleForTesting
    ResourceId getTemporaryDirectory() {
      return this.tempDirectory.get();
    }
  }

  /** A {@link Writer} that can write objects as XML elements. */
  protected static final class XmlWriter<T> extends Writer<Void, T> {
    final Marshaller marshaller;
    private OutputStream os = null;

    public XmlWriter(XmlWriteOperation<T> writeOperation, Marshaller marshaller) {
      super(writeOperation, MimeTypes.TEXT);
      this.marshaller = marshaller;
    }

    /**
     * Creates the output stream that elements will be written to.
     */
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      os = Channels.newOutputStream(channel);
    }

    /**
     * Writes the root element opening tag.
     */
    @Override
    protected void writeHeader() throws Exception {
      String rootElementName = getWriteOperation().getSink().spec.getRootElement();
      os.write(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "<" + rootElementName + ">\n"));
    }

    /**
     * Writes the root element closing tag.
     */
    @Override
    protected void writeFooter() throws Exception {
      String rootElementName = getWriteOperation().getSink().spec.getRootElement();
      os.write(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "\n</" + rootElementName + ">"));
    }

    /**
     * Writes a value to the stream.
     */
    @Override
    public void write(T value) throws Exception {
      marshaller.marshal(value, os);
    }

    /**
     * Return the XmlWriteOperation this write belongs to.
     */
    @Override
    public XmlWriteOperation<T> getWriteOperation() {
      return (XmlWriteOperation<T>) super.getWriteOperation();
    }
  }
}
