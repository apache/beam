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
package org.apache.beam.runners.dataflow.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.transforms.DataflowDisplayDataEvaluator;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;

import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

/**
 * {@link DataflowRunner} specific tests for {@link AvroIO} transforms.
 */
@RunWith(JUnit4.class)
public class DataflowAvroIOTest {
  @Test
  public void testPrimitiveWriteDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    AvroIO.Write.Bound<?> write = AvroIO.Write
        .to("foo")
        .withSchema(Schema.create(Schema.Type.STRING))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("AvroIO.Write should include the file pattern in its primitive transform",
        displayData, hasItem(hasDisplayItem("fileNamePattern")));
  }

  @Test
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    AvroIO.Read.Bound<?> read = AvroIO.Read.from("foo.*")
        .withSchema(Schema.create(Schema.Type.STRING))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("AvroIO.Read should include the file pattern in its primitive transform",
        displayData, hasItem(hasDisplayItem("filePattern")));
  }
}
