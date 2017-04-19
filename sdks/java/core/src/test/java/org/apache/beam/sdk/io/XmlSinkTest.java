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
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.beam.sdk.io.XmlSink.XmlWriteOperation;
import org.apache.beam.sdk.io.XmlSink.XmlWriter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for XmlSink.
 */
@RunWith(JUnit4.class)
public class XmlSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private String testRootElement = "testElement";
  private String testFilePrefix = "/path/to/testPrefix";

  /**
   * An XmlWriter correctly writes objects as Xml elements with an enclosing root element.
   */
  @Test
  public void testXmlWriter() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlWriteOperation<Bird> writeOp =
        XmlIO.<Bird>write()
            .toFilenamePrefix(testFilePrefix)
            .withRecordClass(Bird.class)
            .withRootElement("birds")
            .createSink()
            .createWriteOperation(options);
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
  public void testBuildXmlWriteTransform() {
    XmlIO.Write<Bird> write =
        XmlIO.<Bird>write()
            .toFilenamePrefix(testFilePrefix)
            .withRecordClass(Bird.class)
            .withRootElement(testRootElement);
    assertEquals(Bird.class, write.getRecordClass());
    assertEquals(testRootElement, write.getRootElement());
    assertEquals(testFilePrefix, write.getFilenamePrefix());
  }

  /** Validation ensures no fields are missing. */
  @Test
  public void testValidateXmlSinkMissingRecordClass() {
    thrown.expect(NullPointerException.class);
    XmlIO.<Bird>write()
        .withRootElement(testRootElement)
        .toFilenamePrefix(testFilePrefix)
        .validate(null);
  }

  @Test
  public void testValidateXmlSinkMissingRootElement() {
    thrown.expect(NullPointerException.class);
    XmlIO.<Bird>write().withRecordClass(Bird.class).toFilenamePrefix(testFilePrefix).validate(null);
  }

  @Test
  public void testValidateXmlSinkMissingFilePrefix() {
    thrown.expect(NullPointerException.class);
    XmlIO.<Bird>write().withRecordClass(Bird.class).withRootElement(testRootElement).validate(null);
  }

  /**
   * An XML Sink correctly creates an XmlWriteOperation.
   */
  @Test
  public void testCreateWriteOperations() {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlSink<Bird> sink =
        XmlIO.<Bird>write()
            .withRecordClass(Bird.class)
            .withRootElement(testRootElement)
            .toFilenamePrefix(testFilePrefix)
            .createSink();
    XmlWriteOperation<Bird> writeOp = sink.createWriteOperation(options);
    Path outputPath = new File(testFilePrefix).toPath();
    Path tempPath = new File(writeOp.tempDirectory.get()).toPath();
    assertEquals(outputPath.getParent(), tempPath.getParent());
    assertThat(
        tempPath.getFileName().toString(), containsString("temp-beam-" + outputPath.getFileName()));
  }

  /**
   * An XmlWriteOperation correctly creates an XmlWriter.
   */
  @Test
  public void testCreateWriter() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    XmlWriteOperation<Bird> writeOp =
        XmlIO.<Bird>write()
            .withRecordClass(Bird.class)
            .withRootElement(testRootElement)
            .toFilenamePrefix(testFilePrefix)
            .createSink()
            .createWriteOperation(options);
    XmlWriter<Bird> writer = writeOp.createWriter(options);
    Path outputPath = new File(testFilePrefix).toPath();
    Path tempPath = new File(writer.getWriteOperation().tempDirectory.get()).toPath();
    assertEquals(outputPath.getParent(), tempPath.getParent());
    assertThat(
        tempPath.getFileName().toString(), containsString("temp-beam-" + outputPath.getFileName()));
    assertNotNull(writer.marshaller);
  }

  @Test
  public void testDisplayData() {
    XmlIO.Write<Integer> write = XmlIO.<Integer>write()
        .toFilenamePrefix("foobar")
        .withRootElement("bird")
        .withRecordClass(Integer.class);

    DisplayData displayData = DisplayData.from(write);

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
