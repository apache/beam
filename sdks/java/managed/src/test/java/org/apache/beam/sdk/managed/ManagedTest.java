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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.managed.testing.TestSchemaTransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
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

  public void runTestProviderTest(Managed.ManagedTransform writeOp) {
    PCollection<Row> rows =
        pipeline.apply(Create.of(ROWS)).setRowSchema(SCHEMA).apply(writeOp).getOutput();

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
            .withConfig(ImmutableMap.of("extraString", "abc", "extraInteger", 123));

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
