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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

// CHECKSTYLE.OFF: JavadocStyle
/**
 * A {@link Sink} that outputs records as XML-formatted elements. Writes a {@link PCollection} of
 * records from JAXB-annotated classes to a single file location.
 *
 * <p>Given a PCollection containing records of type T that can be marshalled to XML elements, this
 * Sink will produce a single file consisting of a single root element that contains all of the
 * elements in the PCollection.
 *
 * <p>XML Sinks are created with a base filename to write to, a root element name that will be used
 * for the root element of the output files, and a class to bind to an XML element. This class
 * will be used in the marshalling of records in an input PCollection to their XML representation
 * and must be able to be bound using JAXB annotations (checked at pipeline construction time).
 *
 * <p>XML Sinks can be written to using the {@link Write} transform:
 *
 * <pre>
 * p.apply(Write.to(
 *      XmlSink.ofRecordClass(Type.class)
 *          .withRootElementName(root_element)
 *          .toFilenamePrefix(output_filename)));
 * </pre>
 *
 * <p>For example, consider the following class with JAXB annotations:
 *
 * <pre>
 *  {@literal @}XmlRootElement(name = "word_count_result")
 *  {@literal @}XmlType(propOrder = {"word", "frequency"})
 *  public class WordFrequency {
 *    private String word;
 *    private long frequency;
 *
 *    public WordFrequency() { }
 *
 *    public WordFrequency(String word, long frequency) {
 *      this.word = word;
 *      this.frequency = frequency;
 *    }
 *
 *    public void setWord(String word) {
 *      this.word = word;
 *    }
 *
 *    public void setFrequency(long frequency) {
 *      this.frequency = frequency;
 *    }
 *
 *    public long getFrequency() {
 *      return frequency;
 *    }
 *
 *    public String getWord() {
 *      return word;
 *    }
 *  }
 * </pre>
 *
 * <p>The following will produce XML output with a root element named "words" from a PCollection of
 * WordFrequency objects:
 * <pre>
 * p.apply(Write.to(
 *  XmlSink.ofRecordClass(WordFrequency.class)
 *      .withRootElement("words")
 *      .toFilenamePrefix(output_file)));
 * </pre>
 *
 * <p>The output of which will look like:
 * <pre>
 * {@code
 * <words>
 *
 *  <word_count_result>
 *    <word>decreased</word>
 *    <frequency>1</frequency>
 *  </word_count_result>
 *
 *  <word_count_result>
 *    <word>War</word>
 *    <frequency>4</frequency>
 *  </word_count_result>
 *
 *  <word_count_result>
 *    <word>empress'</word>
 *    <frequency>14</frequency>
 *  </word_count_result>
 *
 *  <word_count_result>
 *    <word>stoops</word>
 *    <frequency>6</frequency>
 *  </word_count_result>
 *
 *  ...
 * </words>
 * }</pre>
 */
// CHECKSTYLE.ON: JavadocStyle
@SuppressWarnings("checkstyle:javadocstyle")
public class XmlSink {
  protected static final String XML_EXTENSION = "xml";

  /**
   * Returns a builder for an XmlSink. You'll need to configure the class to bind, the root
   * element name, and the output file prefix with {@link Bound#ofRecordClass}, {@link
   * Bound#withRootElement}, and {@link Bound#toFilenamePrefix}, respectively.
   */
  public static Bound<?> write() {
    return new Bound<>(null, null, null);
  }

  /**
   * Returns an XmlSink that writes objects as XML entities.
   *
   * <p>Output files will have the name {@literal {baseOutputFilename}-0000i-of-0000n.xml} where n
   * is the number of output bundles that the Dataflow service divides the output into.
   *
   * @param klass the class of the elements to write.
   * @param rootElementName the enclosing root element.
   * @param baseOutputFilename the output filename prefix.
   */
  public static <T> Bound<T> writeOf(
      Class<T> klass, String rootElementName, String baseOutputFilename) {
    return new Bound<>(klass, rootElementName, baseOutputFilename);
  }

  /**
   * A {@link FileBasedSink} that writes objects as XML elements.
   */
  public static class Bound<T> extends FileBasedSink<T> {
    final Class<T> classToBind;
    final String rootElementName;

    private Bound(Class<T> classToBind, String rootElementName, String baseOutputFilename) {
      super(baseOutputFilename, XML_EXTENSION);
      this.classToBind = classToBind;
      this.rootElementName = rootElementName;
    }

    /**
     * Returns an XmlSink that writes objects of the class specified as XML elements.
     *
     * <p>The specified class must be able to be used to create a JAXB context.
     */
    public <T> Bound<T> ofRecordClass(Class<T> classToBind) {
      return new Bound<>(classToBind, rootElementName, baseOutputFilename);
    }

    /**
     * Returns an XmlSink that writes to files with the given prefix.
     *
     * <p>Output files will have the name {@literal {filenamePrefix}-0000i-of-0000n.xml} where n is
     * the number of output bundles that the Dataflow service divides the output into.
     */
    public Bound<T> toFilenamePrefix(String baseOutputFilename) {
      return new Bound<>(classToBind, rootElementName, baseOutputFilename);
    }

    /**
     * Returns an XmlSink that writes XML files with an enclosing root element of the
     * supplied name.
     */
    public Bound<T> withRootElement(String rootElementName) {
      return new Bound<>(classToBind, rootElementName, baseOutputFilename);
    }

    /**
     * Validates that the root element, class to bind to a JAXB context, and filenamePrefix have
     * been set and that the class can be bound in a JAXB context.
     */
    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(classToBind, "Missing a class to bind to a JAXB context.");
      checkNotNull(rootElementName, "Missing a root element name.");
      checkNotNull(baseOutputFilename, "Missing a filename to write to.");
      try {
        JAXBContext.newInstance(classToBind);
      } catch (JAXBException e) {
        throw new RuntimeException("Error binding classes to a JAXB Context.", e);
      }
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
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("rootElement", rootElementName)
            .withLabel("XML Root Element"))
          .addIfNotNull(DisplayData.item("recordClass", classToBind)
            .withLabel("XML Record Class"));
    }
  }

  /**
   * {@link Sink.WriteOperation} for XML {@link Sink}s.
   */
  protected static final class XmlWriteOperation<T> extends FileBasedWriteOperation<T> {
    public XmlWriteOperation(XmlSink.Bound<T> sink) {
      super(sink);
    }

    /**
     * Creates a {@link XmlWriter} with a marshaller for the type it will write.
     */
    @Override
    public XmlWriter<T> createWriter(PipelineOptions options) throws Exception {
      JAXBContext context;
      Marshaller marshaller;
      context = JAXBContext.newInstance(getSink().classToBind);
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
    public XmlSink.Bound<T> getSink() {
      return (XmlSink.Bound<T>) super.getSink();
    }
  }

  /**
   * A {@link Sink.Writer} that can write objects as XML elements.
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
      String rootElementName = getWriteOperation().getSink().rootElementName;
      os.write(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "<" + rootElementName + ">\n"));
    }

    /**
     * Writes the root element closing tag.
     */
    @Override
    protected void writeFooter() throws Exception {
      String rootElementName = getWriteOperation().getSink().rootElementName;
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
