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
package org.apache.beam.sdk.io.json;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JsonIO.Write}. */
@RunWith(JUnit4.class)
public class JsonIOWriteTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule
  public TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void writesUserDefinedTypes() {
    File folder =
        createFolder(AllPrimitiveDataTypes.class.getSimpleName(), "writesUserDefinedTypes");

    PCollection<AllPrimitiveDataTypes> input =
        writePipeline.apply(
            Create.of(
                allPrimitiveDataTypes(false, BigDecimal.TEN, 1.0, 1.0f, 1, 1L, "a"),
                allPrimitiveDataTypes(
                    false, BigDecimal.TEN.add(BigDecimal.TEN), 2.0, 2.0f, 2, 2L, "b"),
                allPrimitiveDataTypes(
                    false,
                    BigDecimal.TEN.add(BigDecimal.TEN).add(BigDecimal.TEN),
                    3.0,
                    3.0f,
                    3,
                    3L,
                    "c")));

    input.apply(JsonIO.<AllPrimitiveDataTypes>write(toFilenamePrefix(folder)).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            containsAll(
                "\"aDouble\":1.0",
                "\"aFloat\":1.0",
                "\"aLong\":1",
                "\"aString\":\"a\"",
                "\"anInteger\":1",
                "\"aDecimal\":10",
                "\"aBoolean\":false"),
            containsAll(
                "\"aDouble\":2.0",
                "\"aFloat\":2.0",
                "\"aLong\":2",
                "\"aString\":\"b\"",
                "\"anInteger\":2",
                "\"aDecimal\":20",
                "\"aBoolean\":false"),
            containsAll(
                "\"aDouble\":3.0",
                "\"aFloat\":3.0",
                "\"aLong\":3",
                "\"aString\":\"c\"",
                "\"anInteger\":3",
                "\"aDecimal\":30",
                "\"aBoolean\":false"));

    readPipeline.run();
  }

  private static SerializableMatcher<String> containsAll(String... needles) {
    class Matcher extends BaseMatcher<String> implements SerializableMatcher<String> {
      @Override
      public boolean matches(Object item) {
        if (!(item instanceof String)) {
          return false;
        }

        String haystack = (String) item;
        for (String needle : needles) {
          if (!haystack.contains(needle)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Contains all of: ");
        description.appendValueList("[", ",", "]", needles);
      }
    }
    return new Matcher();
  }

  private static String toFilenamePrefix(File folder) {
    checkArgument(folder.isDirectory());
    return folder.getAbsolutePath() + "/out";
  }

  private File createFolder(String... paths) {
    try {
      return tempFolder.newFolder(paths);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
