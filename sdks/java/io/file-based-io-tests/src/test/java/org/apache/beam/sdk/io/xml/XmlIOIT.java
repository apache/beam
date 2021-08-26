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

import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.xml.XmlIO}.
 *
 * <p>Run those tests using the command below. Pass in connection information via PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/file-based-io-tests
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--datasetSize=12345",
 *  "--expectedHash=99f23ab",
 *  "--filenamePrefix=output_file_path",
 *  "--charset=UTF-8",
 *  ]'
 *  --tests org.apache.beam.sdk.io.xml.XmlIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class XmlIOIT {

  /** XmlIOIT options. */
  public interface XmlIOITPipelineOptions extends FileBasedIOTestPipelineOptions {
    @Description("Xml file charset name")
    @Default.String("UTF-8")
    String getCharset();

    void setCharset(String charset);
  }

  private static String filenamePrefix;
  private static String bigQueryDataset;
  private static String bigQueryTable;
  private static Integer numberOfTextLines;
  private static Integer datasetSize;
  private static String expectedHash;

  private static final String XMLIOIT_NAMESPACE = XmlIOIT.class.getName();

  private static Charset charset;
  private static InfluxDBSettings settings;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    XmlIOITPipelineOptions options = readIOTestPipelineOptions(XmlIOITPipelineOptions.class);
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    charset = Charset.forName(options.getCharset());
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
    datasetSize = options.getDatasetSize();
    expectedHash = options.getExpectedHash();
    numberOfTextLines = options.getNumberOfRecords();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Test
  public void writeThenReadAll() {
    PCollection<String> testFileNames =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
            .apply("Create xml records", MapElements.via(new LongToBird()))
            .apply(
                "Gather write start time",
                ParDo.of(new TimeMonitor<>(XMLIOIT_NAMESPACE, "writeStart")))
            .apply(
                "Write xml files",
                FileIO.<Bird>write()
                    .via(XmlIO.sink(Bird.class).withRootElement("birds").withCharset(charset))
                    .to(filenamePrefix)
                    .withPrefix("birds")
                    .withSuffix(".xml"))
            .getPerDestinationOutputFilenames()
            .apply(
                "Gather write end time", ParDo.of(new TimeMonitor<>(XMLIOIT_NAMESPACE, "writeEnd")))
            .apply("Get file names", Values.create());

    PCollection<Bird> birds =
        testFileNames
            .apply("Find files", FileIO.matchAll())
            .apply("Read matched files", FileIO.readMatches())
            .apply(
                "Gather read start time",
                ParDo.of(new TimeMonitor<>(XMLIOIT_NAMESPACE, "readStart")))
            .apply(
                "Read xml files",
                XmlIO.<Bird>readFiles()
                    .withRecordClass(Bird.class)
                    .withRootElement("birds")
                    .withRecordElement("bird")
                    .withCharset(charset))
            .apply(
                "Gather read end time", ParDo.of(new TimeMonitor<>(XMLIOIT_NAMESPACE, "readEnd")));

    PCollection<String> consolidatedHashcode =
        birds
            .apply("Map xml records to strings", MapElements.via(new BirdToString()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFileNames.apply(
        "Delete test files",
        ParDo.of(new FileBasedIOITHelper.DeleteFileFn())
            .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishResults(result);
  }

  private void collectAndPublishResults(PipelineResult result) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Timestamp.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        fillMetricSuppliers(uuid, timestamp);
    final IOITMetrics metrics =
        new IOITMetrics(metricSuppliers, result, XMLIOIT_NAMESPACE, uuid, timestamp);
    metrics.publish(bigQueryDataset, bigQueryTable);
    metrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> fillMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeStart");
          long writeEnd = reader.getEndTimeMetric("writeEnd");
          double writeTime = (writeEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "write_time", writeTime);
        });

    suppliers.add(
        reader -> {
          long readStart = reader.getStartTimeMetric("readStart");
          long readEnd = reader.getEndTimeMetric("readEnd");
          double readTime = (readEnd - readStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "read_time", readTime);
        });

    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeStart");
          long readEnd = reader.getEndTimeMetric("readEnd");
          double runTime = (readEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "run_time", runTime);
        });
    if (datasetSize != null) {
      suppliers.add(
          (ignored) -> NamedTestResult.create(uuid, timestamp, "dataset_size", datasetSize));
    }
    return suppliers;
  }

  private static class LongToBird extends SimpleFunction<Long, Bird> {
    @Override
    public Bird apply(Long input) {
      return new Bird("Testing", "Bird number " + input);
    }
  }

  private static class BirdToString extends SimpleFunction<Bird, String> {
    @Override
    public String apply(Bird input) {
      return input.toString();
    }
  }

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
