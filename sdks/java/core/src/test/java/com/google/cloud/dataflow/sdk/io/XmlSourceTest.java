/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static com.google.cloud.dataflow.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static com.google.cloud.dataflow.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Source.Reader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Tests XmlSource.
 */
@RunWith(JUnit4.class)
public class XmlSourceTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  String tinyXML =
      "<trains><train><name>Thomas</name></train><train><name>Henry</name></train>"
      + "<train><name>James</name></train></trains>";

  String xmlWithMultiByteElementName =
      "<දුම්රියන්><දුම්රිය><name>Thomas</name></දුම්රිය><දුම්රිය><name>Henry</name></දුම්රිය>"
      + "<දුම්රිය><name>James</name></දුම්රිය></දුම්රියන්>";

  String xmlWithMultiByteChars =
      "<trains><train><name>Thomas¥</name></train><train><name>Hen¶ry</name></train>"
      + "<train><name>Jamßes</name></train></trains>";

  String trainXML =
      "<trains>"
      + "<train><name>Thomas</name><number>1</number><color>blue</color></train>"
      + "<train><name>Henry</name><number>3</number><color>green</color></train>"
      + "<train><name>Toby</name><number>7</number><color>brown</color></train>"
      + "<train><name>Gordon</name><number>4</number><color>blue</color></train>"
      + "<train><name>Emily</name><number>-1</number><color>red</color></train>"
      + "<train><name>Percy</name><number>6</number><color>green</color></train>"
      + "</trains>";

  String trainXMLWithEmptyTags =
      "<trains>"
      + "<train/>"
      + "<train><name>Thomas</name><number>1</number><color>blue</color></train>"
      + "<train><name>Henry</name><number>3</number><color>green</color></train>"
      + "<train/>"
      + "<train><name>Toby</name><number>7</number><color>brown</color></train>"
      + "<train><name>Gordon</name><number>4</number><color>blue</color></train>"
      + "<train><name>Emily</name><number>-1</number><color>red</color></train>"
      + "<train><name>Percy</name><number>6</number><color>green</color></train>"
      + "</trains>";

  String trainXMLWithAttributes =
      "<trains>"
      + "<train size=\"small\"><name>Thomas</name><number>1</number><color>blue</color></train>"
      + "<train size=\"big\"><name>Henry</name><number>3</number><color>green</color></train>"
      + "<train size=\"small\"><name>Toby</name><number>7</number><color>brown</color></train>"
      + "<train size=\"big\"><name>Gordon</name><number>4</number><color>blue</color></train>"
      + "<train size=\"small\"><name>Emily</name><number>-1</number><color>red</color></train>"
      + "<train size=\"small\"><name>Percy</name><number>6</number><color>green</color></train>"
      + "</trains>";

  String trainXMLWithSpaces =
      "<trains>"
      + "<train><name>Thomas   </name>   <number>1</number><color>blue</color></train>"
      + "<train><name>Henry</name><number>3</number><color>green</color></train>\n"
      + "<train><name>Toby</name><number>7</number><color>  brown  </color></train>  "
      + "<train><name>Gordon</name>   <number>4</number><color>blue</color>\n</train>\t"
      + "<train><name>Emily</name><number>-1</number>\t<color>red</color></train>"
      + "<train>\n<name>Percy</name>   <number>6  </number>   <color>green</color></train>"
      + "</trains>";

  String trainXMLWithAllFeaturesMultiByte =
      "<දුම්රියන්>"
      + "<දුම්රිය/>"
      + "<දුම්රිය size=\"small\"><name> Thomas¥</name><number>1</number><color>blue</color>"
      + "</දුම්රිය>"
      + "<දුම්රිය size=\"big\"><name>He nry</name><number>3</number><color>green</color></දුම්රිය>"
      + "<දුම්රිය size=\"small\"><name>Toby  </name><number>7</number><color>br¶own</color>"
      + "</දුම්රිය>"
      + "<දුම්රිය/>"
      + "<දුම්රිය size=\"big\"><name>Gordon</name><number>4</number><color> blue</color></දුම්රිය>"
      + "<දුම්රිය size=\"small\"><name>Emily</name><number>-1</number><color>red</color></දුම්රිය>"
      + "<දුම්රිය size=\"small\"><name>Percy</name><number>6</number><color>green</color>"
      + "</දුම්රිය>"
      + "</දුම්රියන්>";

  String trainXMLWithAllFeaturesSingleByte =
      "<trains>"
      + "<train/>"
      + "<train size=\"small\"><name> Thomas</name><number>1</number><color>blue</color>"
      + "</train>"
      + "<train size=\"big\"><name>He nry</name><number>3</number><color>green</color></train>"
      + "<train size=\"small\"><name>Toby  </name><number>7</number><color>brown</color>"
      + "</train>"
      + "<train/>"
      + "<train size=\"big\"><name>Gordon</name><number>4</number><color> blue</color></train>"
      + "<train size=\"small\"><name>Emily</name><number>-1</number><color>red</color></train>"
      + "<train size=\"small\"><name>Percy</name><number>6</number><color>green</color>"
      + "</train>"
      + "</trains>";

  @XmlRootElement
  static class Train {
    public static final int TRAIN_NUMBER_UNDEFINED = -1;
    public String name = null;
    public String color = null;
    public int number = TRAIN_NUMBER_UNDEFINED;

    @XmlAttribute(name = "size")
    public String size = null;

    public Train() {}

    public Train(String name, int number, String color, String size) {
      this.name = name;
      this.number = number;
      this.color = color;
      this.size = size;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;
      hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
      hashCode = 31 * hashCode + number;
      hashCode = 31 * hashCode + (color == null ? 0 : name.hashCode());
      hashCode = 31 * hashCode + (size == null ? 0 : name.hashCode());
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Train)) {
        return false;
      }

      Train other = (Train) obj;
      return (name == null || name.equals(other.name)) && (number == other.number)
          && (color == null || color.equals(other.color))
          && (size == null || size.equals(other.size));
    }

    @Override
    public String toString() {
      String str = "Train[";
      boolean first = true;
      if (name != null) {
        str = str + "name=" + name;
        first = false;
      }
      if (number != Integer.MIN_VALUE) {
        if (!first) {
          str = str + ",";
        }
        str = str + "number=" + number;
        first = false;
      }
      if (color != null) {
        if (!first) {
          str = str + ",";
        }
        str = str + "color=" + color;
        first = false;
      }
      if (size != null) {
        if (!first) {
          str = str + ",";
        }
        str = str + "size=" + size;
      }
      str = str + "]";
      return str;
    }
  }

  private List<Train> generateRandomTrainList(int size) {
    String[] names = {"Thomas", "Henry", "Gordon", "Emily", "Toby", "Percy", "Mavis", "Edward",
        "Bertie", "Harold", "Hiro", "Terence", "Salty", "Trevor"};
    int[] numbers = {-1, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    String[] colors = {"red", "blue", "green", "orange", "brown", "black", "white"};
    String[] sizes = {"small", "medium", "big"};

    Random random = new Random(System.currentTimeMillis());

    List<Train> trains = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      trains.add(new Train(names[random.nextInt(names.length - 1)],
          numbers[random.nextInt(numbers.length - 1)], colors[random.nextInt(colors.length - 1)],
          sizes[random.nextInt(sizes.length - 1)]));
    }

    return trains;
  }

  private String trainToXMLElement(Train train) {
    return "<train size=\"" + train.size + "\"><name>" + train.name + "</name><number>"
        + train.number + "</number><color>" + train.color + "</color></train>";
  }

  private File createRandomTrainXML(String fileName, List<Train> trains) throws IOException {
    File file = tempFolder.newFile(fileName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write("<trains>");
      writer.newLine();
      for (Train train : trains) {
        String str = trainToXMLElement(train);
        writer.write(str);
        writer.newLine();
      }
      writer.write("</trains>");
      writer.newLine();
    }
    return file;
  }

  private List<Train> readEverythingFromReader(Reader<Train> reader) throws IOException {
    List<Train> results = new ArrayList<>();
    for (boolean available = reader.start(); available; available = reader.advance()) {
      Train train = reader.getCurrent();
      results.add(train);
    }
    return results;
  }

  @Test
  public void testReadXMLTiny() throws IOException {
    File file = tempFolder.newFile("trainXMLTiny");
    Files.write(file.toPath(), tinyXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(
        new Train("Thomas", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("Henry", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("James", Train.TRAIN_NUMBER_UNDEFINED, null, null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLWithMultiByteChars() throws IOException {
    File file = tempFolder.newFile("trainXMLTiny");
    Files.write(file.toPath(), xmlWithMultiByteChars.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(
        new Train("Thomas¥", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("Hen¶ry", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("Jamßes", Train.TRAIN_NUMBER_UNDEFINED, null, null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  @Ignore(
      "Multi-byte characters in XML are not supported because the parser "
          + "currently does not correctly report byte offsets")
  public void testReadXMLWithMultiByteElementName() throws IOException {
    File file = tempFolder.newFile("trainXMLTiny");
    Files.write(file.toPath(), xmlWithMultiByteElementName.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("දුම්රියන්")
            .withRecordElement("දුම්රිය")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(
        new Train("Thomas", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("Henry", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("James", Train.TRAIN_NUMBER_UNDEFINED, null, null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testSplitWithEmptyBundleAtEnd() throws Exception {
    File file = tempFolder.newFile("trainXMLTiny");
    Files.write(file.toPath(), tinyXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(10);
    List<? extends FileBasedSource<Train>> splits = source.splitIntoBundles(50, null);

    assertTrue(splits.size() > 2);

    List<Train> results = new ArrayList<>();
    for (FileBasedSource<Train> split : splits) {
      results.addAll(readEverythingFromReader(split.createReader(null)));
    }

    List<Train> expectedResults = ImmutableList.of(
        new Train("Thomas", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("Henry", Train.TRAIN_NUMBER_UNDEFINED, null, null),
        new Train("James", Train.TRAIN_NUMBER_UNDEFINED, null, null));

    assertThat(
        trainsToStrings(expectedResults), containsInAnyOrder(trainsToStrings(results).toArray()));
  }

  List<String> trainsToStrings(List<Train> input) {
    List<String> strings = new ArrayList<>();
    for (Object data : input) {
      strings.add(data.toString());
    }
    return strings;
  }

  @Test
  public void testReadXMLSmall() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults =
        ImmutableList.of(new Train("Thomas", 1, "blue", null), new Train("Henry", 3, "green", null),
            new Train("Toby", 7, "brown", null), new Train("Gordon", 4, "blue", null),
            new Train("Emily", -1, "red", null), new Train("Percy", 6, "green", null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLNoRootElement() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRecordElement("train")
            .withRecordClass(Train.class);

    exception.expect(NullPointerException.class);
    exception.expectMessage(
        "rootElement is null. Use builder method withRootElement() to set this.");
    readEverythingFromReader(source.createReader(null));
  }

  @Test
  public void testReadXMLNoRecordElement() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordClass(Train.class);

    exception.expect(NullPointerException.class);
    exception.expectMessage(
        "recordElement is null. Use builder method withRecordElement() to set this.");
    readEverythingFromReader(source.createReader(null));
  }

  @Test
  public void testReadXMLNoRecordClass() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train");

    exception.expect(NullPointerException.class);
    exception.expectMessage(
        "recordClass is null. Use builder method withRecordClass() to set this.");
    readEverythingFromReader(source.createReader(null));
  }

  @Test
  public void testReadXMLIncorrectRootElement() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("something")
            .withRecordElement("train")
            .withRecordClass(Train.class);

    exception.expectMessage("Unexpected close tag </trains>; expected </something>.");
    readEverythingFromReader(source.createReader(null));
  }

  @Test
  public void testReadXMLIncorrectRecordElement() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("something")
            .withRecordClass(Train.class);

    assertEquals(readEverythingFromReader(source.createReader(null)), new ArrayList<Train>());
  }

  @XmlRootElement
  private static class WrongTrainType {
    @SuppressWarnings("unused")
    public String something;
  }

  @Test
  public void testReadXMLInvalidRecordClass() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<WrongTrainType> source =
        XmlSource.<WrongTrainType>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(WrongTrainType.class);

    exception.expect(RuntimeException.class);

    // JAXB internationalizes the error message. So this is all we can match for.
    exception.expectMessage(both(containsString("name")).and(Matchers.containsString("something")));
    try (Reader<WrongTrainType> reader = source.createReader(null)) {

      List<WrongTrainType> results = new ArrayList<>();
      for (boolean available = reader.start(); available; available = reader.advance()) {
        WrongTrainType train = reader.getCurrent();
        results.add(train);
      }
    }
  }

  @Test
  public void testReadXMLNoBundleSize() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class);

    List<Train> expectedResults =
        ImmutableList.of(new Train("Thomas", 1, "blue", null), new Train("Henry", 3, "green", null),
            new Train("Toby", 7, "brown", null), new Train("Gordon", 4, "blue", null),
            new Train("Emily", -1, "red", null), new Train("Percy", 6, "green", null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }


  @Test
  public void testReadXMLWithEmptyTags() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXMLWithEmptyTags.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(new Train("Thomas", 1, "blue", null),
        new Train("Henry", 3, "green", null), new Train("Toby", 7, "brown", null),
        new Train("Gordon", 4, "blue", null), new Train("Emily", -1, "red", null),
        new Train("Percy", 6, "green", null), new Train(), new Train());

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLSmallDataflow() throws IOException {
    Pipeline p = TestPipeline.create();

    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXML.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    PCollection<Train> output = p.apply(Read.from(source).named("ReadFileData"));

    List<Train> expectedResults =
        ImmutableList.of(new Train("Thomas", 1, "blue", null), new Train("Henry", 3, "green", null),
            new Train("Toby", 7, "brown", null), new Train("Gordon", 4, "blue", null),
            new Train("Emily", -1, "red", null), new Train("Percy", 6, "green", null));

    DataflowAssert.that(output).containsInAnyOrder(expectedResults);
    p.run();
  }

  @Test
  public void testReadXMLWithAttributes() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXMLWithAttributes.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(new Train("Thomas", 1, "blue", "small"),
        new Train("Henry", 3, "green", "big"), new Train("Toby", 7, "brown", "small"),
        new Train("Gordon", 4, "blue", "big"), new Train("Emily", -1, "red", "small"),
        new Train("Percy", 6, "green", "small"));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLWithWhitespaces() throws IOException {
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXMLWithSpaces.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    List<Train> expectedResults = ImmutableList.of(new Train("Thomas   ", 1, "blue", null),
        new Train("Henry", 3, "green", null), new Train("Toby", 7, "  brown  ", null),
        new Train("Gordon", 4, "blue", null), new Train("Emily", -1, "red", null),
        new Train("Percy", 6, "green", null));

    assertThat(
        trainsToStrings(expectedResults),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLLarge() throws IOException {
    String fileName = "temp.xml";
    List<Train> trains = generateRandomTrainList(100);
    File file = createRandomTrainXML(fileName, trains);

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);

    assertThat(
        trainsToStrings(trains),
        containsInAnyOrder(
            trainsToStrings(readEverythingFromReader(source.createReader(null))).toArray()));
  }

  @Test
  public void testReadXMLLargeDataflow() throws IOException {
    String fileName = "temp.xml";
    List<Train> trains = generateRandomTrainList(100);
    File file = createRandomTrainXML(fileName, trains);

    Pipeline p = TestPipeline.create();
    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(1024);
    PCollection<Train> output = p.apply(Read.from(source).named("ReadFileData"));

    DataflowAssert.that(output).containsInAnyOrder(trains);
    p.run();
  }

  @Test
  public void testSplitWithEmptyBundles() throws Exception {
    String fileName = "temp.xml";
    List<Train> trains = generateRandomTrainList(10);
    File file = createRandomTrainXML(fileName, trains);

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(10);
    List<? extends FileBasedSource<Train>> splits = source.splitIntoBundles(100, null);

    assertTrue(splits.size() > 2);

    List<Train> results = new ArrayList<>();
    for (FileBasedSource<Train> split : splits) {
      results.addAll(readEverythingFromReader(split.createReader(null)));
    }

    assertThat(trainsToStrings(trains), containsInAnyOrder(trainsToStrings(results).toArray()));
  }

  @Test
  public void testXMLWithSplits() throws Exception {
    String fileName = "temp.xml";
    List<Train> trains = generateRandomTrainList(100);
    File file = createRandomTrainXML(fileName, trains);

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(10);
    List<? extends FileBasedSource<Train>> splits = source.splitIntoBundles(256, null);

    // Not a trivial split
    assertTrue(splits.size() > 2);

    List<Train> results = new ArrayList<>();
    for (FileBasedSource<Train> split : splits) {
      results.addAll(readEverythingFromReader(split.createReader(null)));
    }
    assertThat(trainsToStrings(trains), containsInAnyOrder(trainsToStrings(results).toArray()));
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    String fileName = "temp.xml";
    List<Train> trains = generateRandomTrainList(100);
    File file = createRandomTrainXML(fileName, trains);

    XmlSource<Train> fileSource =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class)
            .withMinBundleSize(10);

    List<? extends FileBasedSource<Train>> splits =
        fileSource.splitIntoBundles(file.length() / 3, null);
    for (BoundedSource<Train> splitSource : splits) {
      int numItems = readEverythingFromReader(splitSource.createReader(null)).size();
      // Should not split while unstarted.
      assertSplitAtFractionFails(splitSource, 0, 0.7, options);
      assertSplitAtFractionSucceedsAndConsistent(splitSource, 1, 0.7, options);
      assertSplitAtFractionSucceedsAndConsistent(splitSource, 15, 0.7, options);
      assertSplitAtFractionFails(splitSource, 0, 0.0, options);
      assertSplitAtFractionFails(splitSource, 20, 0.3, options);
      assertSplitAtFractionFails(splitSource, numItems, 1.0, options);

      // After reading 100 elements we will be approximately at position
      // 0.99 * (endOffset - startOffset) hence trying to split at fraction 0.9 will be
      // unsuccessful.
      assertSplitAtFractionFails(splitSource, numItems, 0.9, options);

      // Following passes since we can always find a fraction that is extremely close to 1 such that
      // the position suggested by the fraction will be larger than the position the reader is at
      // after reading "items - 1" elements.
      // This also passes for "numItemsToReadBeforeSplit = items" if the position at suggested
      // fraction is larger than the position the reader is at after reading all "items" elements
      // (i.e., the start position of the last element). This is true for most cases but will not
      // be true if reader position is only one less than the end position. (i.e., the last element
      // of the bundle start at the last byte that belongs to the bundle).
      assertSplitAtFractionSucceedsAndConsistent(splitSource, numItems - 1, 0.999, options);
    }
  }

  @Test
  public void testSplitAtFractionExhaustiveSingleByte() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXMLWithAllFeaturesSingleByte.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("trains")
            .withRecordElement("train")
            .withRecordClass(Train.class);
    assertSplitAtFractionExhaustive(source, options);
  }

  @Test
  @Ignore(
      "Multi-byte characters in XML are not supported because the parser "
      + "currently does not correctly report byte offsets")
  public void testSplitAtFractionExhaustiveMultiByte() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = tempFolder.newFile("trainXMLSmall");
    Files.write(file.toPath(), trainXMLWithAllFeaturesMultiByte.getBytes(StandardCharsets.UTF_8));

    XmlSource<Train> source =
        XmlSource.<Train>from(file.toPath().toString())
            .withRootElement("දුම්රියන්")
            .withRecordElement("දුම්රිය")
            .withRecordClass(Train.class);
    assertSplitAtFractionExhaustive(source, options);
  }

  @Test
  public void testReadXMLFilePattern() throws IOException {
    List<Train> trains1 = generateRandomTrainList(20);
    File file = createRandomTrainXML("temp1.xml", trains1);
    List<Train> trains2 = generateRandomTrainList(10);
    createRandomTrainXML("temp2.xml", trains2);
    List<Train> trains3 = generateRandomTrainList(15);
    createRandomTrainXML("temp3.xml", trains3);
    generateRandomTrainList(8);
    createRandomTrainXML("otherfile.xml", trains1);

    Pipeline p = TestPipeline.create();

    XmlSource<Train> source = XmlSource.<Train>from(file.getParent() + "/"
        + "temp*.xml")
                                  .withRootElement("trains")
                                  .withRecordElement("train")
                                  .withRecordClass(Train.class)
                                  .withMinBundleSize(1024);
    PCollection<Train> output = p.apply(Read.from(source).named("ReadFileData"));

    List<Train> expectedResults = new ArrayList<>();
    expectedResults.addAll(trains1);
    expectedResults.addAll(trains2);
    expectedResults.addAll(trains3);

    DataflowAssert.that(output).containsInAnyOrder(expectedResults);
    p.run();
  }
}
