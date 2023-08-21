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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileReadSchemaTransformProvider.FILEPATTERN_ROW_FIELD_NAME;
import static org.apache.beam.sdk.transforms.Contextful.fn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class LineReadSchemaTransformFormatProviderTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public TestName testName = new TestName();

  protected String getFormat() {
    return new LineReadSchemaTransformFormatProvider().identifier();
  }

  protected String getFilePath() {
    return getFolder() + "/test";
  }

  protected String getFolder() {
    try {
      return tmpFolder.newFolder(getFormat(), testName.getMethodName()).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void testReadStrings() {
    List<String> values = new ArrayList<>();

    String filePath = getFilePath();

    for (int i = 0; i < 10; i++) {
      values.add("line #" + i);
    }

    writePipeline.apply(Create.of(values)).apply(TextIO.write().to(filePath));
    writePipeline.run().waitUntilFinish();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setFilepattern(filePath + "*")
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);
    PCollection<String> outputStrings =
        output
            .get(FileReadSchemaTransformProvider.OUTPUT_TAG)
            .apply(MapElements.into(TypeDescriptors.strings()).via(row -> row.getString("line")));

    PAssert.that(outputStrings).containsInAnyOrder(values);
    readPipeline.run().waitUntilFinish();
  }

  private static class CreateKeyStringPair extends SimpleFunction<Long, KV<Integer, String>> {
    @Override
    public KV<Integer, String> apply(Long l) {
      String line = "dynamic destination line #" + l.intValue();

      return KV.of(l.intValue() + 1, line);
    }
  }

  @Test
  public void testStreamingRead() {
    String folder = getFolder();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setFilepattern(folder + "/test_*")
            .setPollIntervalMillis(100L)
            .setTerminateAfterSecondsSinceNewOutput(3L)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);

    // Write to three different files (test_1..., test_2..., test_3)
    // All three new files should be picked up and read.
    readPipeline
        .apply(GenerateSequence.from(0).to(3).withRate(1, Duration.millis(300)))
        .apply(
            Window.<Long>into(FixedWindows.of(Duration.millis(100)))
                .withAllowedLateness(Duration.ZERO)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes())
        .apply("Create Key-String pairs", MapElements.via(new CreateKeyStringPair()))
        .setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
        .apply(
            FileIO.<Integer, KV<Integer, String>>writeDynamic()
                .by(KV::getKey)
                .via(fn(KV<Integer, String>::getValue), TextIO.sink())
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".txt"))
                .withDestinationCoder(VarIntCoder.of())
                .withNumShards(1));

    PCollection<String> outputStrings =
        output
            .get(FileReadSchemaTransformProvider.OUTPUT_TAG)
            .apply(
                "Get strings",
                MapElements.into(TypeDescriptors.strings()).via(row -> row.getString("line")));

    List<String> expectedStrings =
        Arrays.asList(0, 1, 2).stream()
            .map(num -> "dynamic destination line #" + num)
            .collect(Collectors.toList());

    PAssert.that(outputStrings).containsInAnyOrder(expectedStrings);
    readPipeline.run();
  }

  @Test
  public void testReadWithPCollectionOfFilepatterns() {
    String folder = getFolder();

    // Write rows to dynamic destinations (test_1.., test_2.., test_3..)
    writePipeline
        .apply(Create.of(Arrays.asList(0L, 1L, 2L)))
        .apply(MapElements.via(new CreateKeyStringPair()))
        .setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
        .apply(
            FileIO.<Integer, KV<Integer, String>>writeDynamic()
                .by(KV::getKey)
                .via(fn(KV<Integer, String>::getValue), TextIO.sink())
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".txt"))
                .withDestinationCoder(VarIntCoder.of())
                .withNumShards(1));
    writePipeline.run().waitUntilFinish();

    // We will get filepatterns from the input PCollection, so don't set filepattern field here
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder().setFormat(getFormat()).build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    // Create an PCollection<Row> of filepatterns and feed into the read transform
    Schema patternSchema = Schema.of(Field.of(FILEPATTERN_ROW_FIELD_NAME, FieldType.STRING));
    PCollection<Row> filepatterns =
        readPipeline
            .apply(
                Create.of(
                    Arrays.asList(
                        folder + "/test_1-*", folder + "/test_2-*", folder + "/test_3-*")))
            .apply(
                "Create Rows of filepatterns",
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        pattern ->
                            Row.withSchema(patternSchema)
                                .withFieldValue("filepattern", pattern)
                                .build()))
            .setRowSchema(patternSchema);

    PCollectionRowTuple output =
        PCollectionRowTuple.of(FileReadSchemaTransformProvider.INPUT_TAG, filepatterns)
            .apply(readTransform);

    PCollection<String> outputStrings =
        output
            .get(FileReadSchemaTransformProvider.OUTPUT_TAG)
            .apply(MapElements.into(TypeDescriptors.strings()).via(row -> row.getString("line")));
    List<String> expectedStrings =
        Arrays.asList(0, 1, 2).stream()
            .map(num -> "dynamic destination line #" + num)
            .collect(Collectors.toList());

    PAssert.that(outputStrings).containsInAnyOrder(expectedStrings);
    readPipeline.run();
  }
}
