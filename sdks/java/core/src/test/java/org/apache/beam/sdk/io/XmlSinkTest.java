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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.io.XmlSink.XmlWriteOperation;
import org.apache.beam.sdk.io.XmlSink.XmlWriter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.common.collect.Lists;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Tests for XmlSink.
 */
@RunWith(JUnit4.class)
public class XmlSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Class<Bird> testClass = Bird.class;
  private String testRootElement = "testElement";
  private String testFilePrefix = "testPrefix";

  /**
   * An XmlWriter correctly writes objects as Xml elements with an enclosing root element.
   */
  @Test
  public void testXmlWriter() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlWriteOperation<Bird> writeOp =
        XmlSink.writeOf(Bird.class, "birds", testFilePrefix).createWriteOperation(options);
    XmlWriter<Bird> writer = writeOp.createWriter(options);

    List<Bird> bundle =
        Lists.newArrayList(new Bird("bemused", "robin"), new Bird("evasive", "goose"));
    List<String> lines = Arrays.asList("<birds>", "<bird>", "<species>robin</species>",
        "<adjective>bemused</adjective>", "</bird>", "<bird>", "<species>goose</species>",
        "<adjective>evasive</adjective>", "</bird>", "</birds>");
    runTestWrite(writer, bundle, lines);
  }

  /**
   * Builder methods correctly initialize an XML Sink.
   */
  @Test
  public void testBuildXmlSink() {
    XmlSink.Bound<Bird> sink =
        XmlSink.write()
            .toFilenamePrefix(testFilePrefix)
            .ofRecordClass(testClass)
            .withRootElement(testRootElement);
    assertEquals(testClass, sink.classToBind);
    assertEquals(testRootElement, sink.rootElementName);
    assertEquals(testFilePrefix, sink.baseOutputFilename);
  }

  /**
   * Alternate builder method correctly initializes an XML Sink.
   */
  @Test
  public void testBuildXmlSinkDirect() {
    XmlSink.Bound<Bird> sink =
        XmlSink.writeOf(Bird.class, testRootElement, testFilePrefix);
    assertEquals(testClass, sink.classToBind);
    assertEquals(testRootElement, sink.rootElementName);
    assertEquals(testFilePrefix, sink.baseOutputFilename);
  }

  /**
   * Validation ensures no fields are missing.
   */
  @Test
  public void testValidateXmlSinkMissingFields() {
    XmlSink.Bound<Bird> sink;
    sink = XmlSink.writeOf(null, testRootElement, testFilePrefix);
    validateAndFailIfSucceeds(sink, NullPointerException.class);
    sink = XmlSink.writeOf(testClass, null, testFilePrefix);
    validateAndFailIfSucceeds(sink, NullPointerException.class);
    sink = XmlSink.writeOf(testClass, testRootElement, null);
    validateAndFailIfSucceeds(sink, NullPointerException.class);
  }

  /**
   * Call validate and fail if validation does not throw the expected exception.
   */
  private <T> void validateAndFailIfSucceeds(
      XmlSink.Bound<T> sink, Class<? extends Exception> expected) {
    thrown.expect(expected);
    PipelineOptions options = PipelineOptionsFactory.create();
    sink.validate(options);
  }

  /**
   * An XML Sink correctly creates an XmlWriteOperation.
   */
  @Test
  public void testCreateWriteOperations() {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlSink.Bound<Bird> sink =
        XmlSink.writeOf(testClass, testRootElement, testFilePrefix);
    XmlWriteOperation<Bird> writeOp = sink.createWriteOperation(options);
    assertEquals(testClass, writeOp.getSink().classToBind);
    assertEquals(testFilePrefix, writeOp.getSink().baseOutputFilename);
    assertEquals(testRootElement, writeOp.getSink().rootElementName);
    assertEquals(XmlSink.XML_EXTENSION, writeOp.getSink().extension);
    assertEquals(testFilePrefix, writeOp.baseTemporaryFilename);
  }

  /**
   * An XmlWriteOperation correctly creates an XmlWriter.
   */
  @Test
  public void testCreateWriter() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlWriteOperation<Bird> writeOp =
        XmlSink.writeOf(testClass, testRootElement, testFilePrefix)
            .createWriteOperation(options);
    XmlWriter<Bird> writer = writeOp.createWriter(options);
    assertEquals(testFilePrefix, writer.getWriteOperation().baseTemporaryFilename);
    assertEquals(testRootElement, writer.getWriteOperation().getSink().rootElementName);
    assertNotNull(writer.marshaller);
  }

  @Test
  public void testDisplayData() {
    XmlSink.Bound<Integer> sink = XmlSink.write()
        .toFilenamePrefix("foobar")
        .withRootElement("bird")
        .ofRecordClass(Integer.class);

    DisplayData displayData = DisplayData.from(sink);

    assertThat(displayData, hasDisplayItem("fileNamePattern", "foobar-SSSSS-of-NNNNN.xml"));
    assertThat(displayData, hasDisplayItem("rootElement", "bird"));
    assertThat(displayData, hasDisplayItem("recordClass", Integer.class));
  }

  /**
   * Write a bundle with an XmlWriter and verify the output is expected.
   */
  private <T> void runTestWrite(XmlWriter<T> writer, List<T> bundle, List<String> expected)
      throws Exception {
    File tmpFile = tmpFolder.newFile("foo.txt");
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile)) {
      writeBundle(writer, bundle, fileOutputStream.getChannel());
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(tmpFile))) {
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        line = line.trim();
        if (line.length() > 0) {
          lines.add(line);
        }
      }
      assertEquals(expected, lines);
    }
  }

  /**
   * Write a bundle with an XmlWriter.
   */
  private <T> void writeBundle(XmlWriter<T> writer, List<T> elements, WritableByteChannel channel)
      throws Exception {
    writer.prepareWrite(channel);
    writer.writeHeader();
    for (T elem : elements) {
      writer.write(elem);
    }
    writer.writeFooter();
  }

  /**
   * Test JAXB annotated class.
   */
  @SuppressWarnings("unused")
  @XmlRootElement(name = "bird")
  @XmlType(propOrder = {"name", "adjective"})
  private static final class Bird {
    private String name;
    private String adjective;

    @XmlElement(name = "species")
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getAdjective() {
      return adjective;
    }

    public void setAdjective(String adjective) {
      this.adjective = adjective;
    }

    public Bird() {}

    public Bird(String adjective, String name) {
      this.adjective = adjective;
      this.name = name;
    }
  }
}
