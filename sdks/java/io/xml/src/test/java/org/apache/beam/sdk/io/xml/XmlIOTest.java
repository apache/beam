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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link XmlIO}. */
@RunWith(JUnit4.class)
public class XmlIOTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public TestPipeline mainPipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final List<Bird> BIRDS =
      Lists.newArrayList(
          new Bird("bemused", "robin"), new Bird("evasive", "goose"), new Bird("bréche", "pinçon"));

  enum Method {
    SINK_AND_READ_FILES,
    WRITE_AND_READ
  }

  @Test
  public void testXmlWriteThenReadViaSinkAndReadFilesUTF8() {
    testWriteThenRead(Method.SINK_AND_READ_FILES, BIRDS, StandardCharsets.UTF_8);
  }

  @Test
  public void testXmlWriteThenReadViaSinkAndReadFilesISO8859() {
    testWriteThenRead(Method.SINK_AND_READ_FILES, BIRDS, StandardCharsets.ISO_8859_1);
  }

  @Test
  public void testXmlWriteThenReadViaWriteAndReadUTF8() {
    testWriteThenRead(Method.WRITE_AND_READ, BIRDS, StandardCharsets.UTF_8);
  }

  @Test
  public void testXmlWriteThenReadViaWriteAndReadISO8859() {
    testWriteThenRead(Method.WRITE_AND_READ, BIRDS, StandardCharsets.ISO_8859_1);
  }

  private void testWriteThenRead(Method method, List<Bird> birds, Charset charset) {
    switch (method) {
      case SINK_AND_READ_FILES:
        PCollection<Bird> writeThenRead =
            mainPipeline
                .apply(Create.of(birds))
                .apply(
                    FileIO.<Bird>write()
                        .via(XmlIO.sink(Bird.class).withRootElement("birds").withCharset(charset))
                        .to(tmpFolder.getRoot().getAbsolutePath())
                        .withPrefix("birds")
                        .withSuffix(".xml"))
                .getPerDestinationOutputFilenames()
                .apply(Values.create())
                .apply(FileIO.matchAll())
                .apply(FileIO.readMatches())
                .apply(
                    XmlIO.<Bird>readFiles()
                        .withRecordClass(Bird.class)
                        .withRootElement("birds")
                        .withRecordElement("bird")
                        .withCharset(charset));

        PAssert.that(writeThenRead).containsInAnyOrder(birds);

        mainPipeline.run();
        break;

      case WRITE_AND_READ:
        mainPipeline
            .apply(Create.of(birds))
            .apply(
                XmlIO.<Bird>write()
                    .to(new File(tmpFolder.getRoot(), "birds").getAbsolutePath())
                    .withRecordClass(Bird.class)
                    .withRootElement("birds")
                    .withCharset(charset));
        mainPipeline.run();

        PCollection<Bird> readBack =
            readPipeline.apply(
                XmlIO.<Bird>read()
                    .from(
                        readPipeline.newProvider(
                            new File(tmpFolder.getRoot(), "birds").getAbsolutePath() + "*"))
                    .withRecordClass(Bird.class)
                    .withRootElement("birds")
                    .withRecordElement("bird")
                    .withCharset(charset));

        PAssert.that(readBack).containsInAnyOrder(birds);

        readPipeline.run();
        break;
    }
  }

  @Test
  public void testWriteThenReadLarger() {
    List<Bird> birds = Lists.newArrayList();
    for (int i = 0; i < 100; ++i) {
      birds.add(new Bird("Testing", "Bird number " + i));
    }
    mainPipeline
        .apply(Create.of(birds))
        .apply(
            FileIO.<Bird>write()
                .via(XmlIO.sink(Bird.class).withRootElement("birds"))
                .to(tmpFolder.getRoot().getAbsolutePath())
                .withPrefix("birds")
                .withSuffix(".xml")
                .withNumShards(1));
    mainPipeline.run();

    PCollection<Bird> readBack =
        readPipeline.apply(
            XmlIO.<Bird>read()
                .from(new File(tmpFolder.getRoot(), "birds").getAbsolutePath() + "*")
                .withRecordClass(Bird.class)
                .withRootElement("birds")
                .withRecordElement("bird")
                .withMinBundleSize(100));

    PAssert.that(readBack).containsInAnyOrder(birds);

    readPipeline.run();
  }

  @Test
  public void testDisplayData() {
    DisplayData displayData =
        DisplayData.from(
            XmlIO.<Integer>read()
                .from("foo.xml")
                .withRootElement("bird")
                .withRecordElement("cat")
                .withMinBundleSize(1234)
                .withRecordClass(Integer.class));

    Assert.assertThat(displayData, hasDisplayItem("filePattern", "foo.xml"));
    Assert.assertThat(displayData, hasDisplayItem("rootElement", "bird"));
    Assert.assertThat(displayData, hasDisplayItem("recordElement", "cat"));
    Assert.assertThat(displayData, hasDisplayItem("recordClass", Integer.class));
    Assert.assertThat(displayData, hasDisplayItem("minBundleSize", 1234));
  }

  @Test
  public void testWriteDisplayData() {
    XmlIO.Write<Integer> write =
        XmlIO.<Integer>write().withRootElement("bird").withRecordClass(Integer.class);

    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("rootElement", "bird"));
    assertThat(displayData, hasDisplayItem("recordClass", Integer.class));
  }

  /** Test JAXB annotated class. */
  @SuppressWarnings("unused")
  @XmlRootElement(name = "bird")
  @XmlType(propOrder = {"name", "adjective"})
  private static final class Bird implements Serializable {
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

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Bird bird = (Bird) o;

      if (!name.equals(bird.name)) {
        return false;
      }
      return adjective.equals(bird.adjective);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + adjective.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return String.format("Bird: %s, %s", name, adjective);
    }
  }
}
