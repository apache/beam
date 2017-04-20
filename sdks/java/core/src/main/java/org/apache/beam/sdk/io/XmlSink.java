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

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;

/** Implementation of {@link XmlIO#write}. */
class XmlSink<T> extends FileBasedSink<T> {
  protected static final String XML_EXTENSION = "xml";

  private final XmlIO.Write<T> spec;

  XmlSink(XmlIO.Write<T> spec) {
    super(spec.getFilenamePrefix(), XML_EXTENSION);
    this.spec = spec;
  }

  /**
   * Validates that the root element, class to bind to a JAXB context, and filenamePrefix have
   * been set and that the class can be bound in a JAXB context.
   */
  @Override
  public void validate(PipelineOptions options) {
    spec.validate(null);
  }

  /**
   * Creates an {@link XmlWriteOperation}.
   */
  @Override
  public XmlWriteOperation<T> createWriteOperation(PipelineOptions options) {
    return new XmlWriteOperation<>(this);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    spec.populateDisplayData(builder);
  }

  void populateFileBasedDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }

  /**
   * {@link FileBasedSink.FileBasedWriteOperation} for XML {@link FileBasedSink}s.
   */
  protected static final class XmlWriteOperation<T> extends FileBasedWriteOperation<T> {
    public XmlWriteOperation(XmlSink<T> sink) {
      super(sink);
    }

    /**
     * Creates a {@link XmlWriter} with a marshaller for the type it will write.
     */
    @Override
    public XmlWriter<T> createWriter(PipelineOptions options) throws Exception {
      JAXBContext context;
      Marshaller marshaller;
      context = JAXBContext.newInstance(getSink().spec.getRecordClass());
      marshaller = context.createMarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
      marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
      return new XmlWriter<>(this, marshaller);
    }

    /**
     * Return the XmlSink.Bound for this write operation.
     */
    @Override
    public XmlSink<T> getSink() {
      return (XmlSink<T>) super.getSink();
    }
  }

  /**
   * A {@link FileBasedWriter} that can write objects as XML elements.
   */
  protected static final class XmlWriter<T> extends FileBasedWriter<T> {
    final Marshaller marshaller;
    private OutputStream os = null;

    public XmlWriter(XmlWriteOperation<T> writeOperation, Marshaller marshaller) {
      super(writeOperation);
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
