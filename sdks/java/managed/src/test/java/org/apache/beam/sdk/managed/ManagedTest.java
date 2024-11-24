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
package org.apache.beam.sdk.managed;

import static org.junit.Assert.assertThrows;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.testing.TestSchemaTransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManagedTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInvalidTransform() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("An unsupported source was specified");
    Managed.read("nonexistent-source");

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("An unsupported sink was specified");
    Managed.write("nonexistent-sink");
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final Schema SCHEMA =
      Schema.builder().addStringField("str").addInt32Field("int").build();
  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA).withFieldValue("str", "a").withFieldValue("int", 1).build(),
          Row.withSchema(SCHEMA).withFieldValue("str", "b").withFieldValue("int", 2).build(),
          Row.withSchema(SCHEMA).withFieldValue("str", "c").withFieldValue("int", 3).build());

  @Test
  public void testResolveInputToPCollectionRowTuple() {
    Pipeline p = Pipeline.create();
    List<PInput> inputTypes =
        Arrays.asList(
            PBegin.in(p),
            p.apply(Create.of(ROWS).withRowSchema(SCHEMA)),
            PCollectionRowTuple.of("pcoll", p.apply(Create.of(ROWS).withRowSchema(SCHEMA))));

    List<PInput> badInputTypes =
        Arrays.asList(
            p.apply(Create.of(1, 2, 3)),
            p.apply(Create.of(ROWS)),
            PCollectionTuple.of("pcoll", p.apply(Create.of(ROWS))));

    for (PInput input : inputTypes) {
      Managed.ManagedTransform.resolveInput(input);
    }
    for (PInput badInput : badInputTypes) {
      assertThrows(
          IllegalArgumentException.class, () -> Managed.ManagedTransform.resolveInput(badInput));
    }
  }

  public void runTestProviderTest(Managed.ManagedTransform writeOp) {
    PCollection<Row> rows =
        pipeline.apply(Create.of(ROWS)).setRowSchema(SCHEMA).apply(writeOp).getSinglePCollection();

    Schema outputSchema = rows.getSchema();
    PAssert.that(rows)
        .containsInAnyOrder(
            ROWS.stream()
                .map(
                    row ->
                        Row.withSchema(outputSchema)
                            .addValues(row.getValues())
                            .addValue("abc")
                            .addValue(123)
                            .build())
                .collect(Collectors.toList()));
    pipeline.run();
  }

  @Test
  public void testManagedTestProviderWithConfigMap() {
    Managed.ManagedTransform writeOp =
        Managed.write(Managed.ICEBERG)
            .toBuilder()
            .setIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .build()
            .withSupportedIdentifiers(Arrays.asList(TestSchemaTransformProvider.IDENTIFIER))
            .withConfig(ImmutableMap.of("extra_string", "abc", "extra_integer", 123));

    runTestProviderTest(writeOp);
  }

  @Test
  public void testManagedTestProviderWithConfigFile() throws Exception {
    String yamlConfigPath =
        Paths.get(getClass().getClassLoader().getResource("test_config.yaml").toURI())
            .toFile()
            .getAbsolutePath();

    Managed.ManagedTransform writeOp =
        Managed.write(Managed.ICEBERG)
            .toBuilder()
            .setIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .build()
            .withSupportedIdentifiers(Arrays.asList(TestSchemaTransformProvider.IDENTIFIER))
            .withConfigUrl(yamlConfigPath);

    runTestProviderTest(writeOp);
  }
}
