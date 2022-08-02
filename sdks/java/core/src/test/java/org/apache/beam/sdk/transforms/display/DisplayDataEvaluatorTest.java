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
package org.apache.beam.sdk.transforms.display;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

import java.io.Serializable;
import java.util.Set;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DisplayDataEvaluator}. */
@RunWith(JUnit4.class)
public class DisplayDataEvaluatorTest implements Serializable {

  @Test
  public void testCompositeTransform() {
    PTransform<? super PCollection<String>, ? super POutput> myTransform =
        new PTransform<PCollection<String>, POutput>() {
          @Override
          public PCollection<String> expand(PCollection<String> input) {
            return input.apply(
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        c.output(c.element());
                      }

                      @Override
                      public void populateDisplayData(DisplayData.Builder builder) {
                        builder.add(DisplayData.item("primitiveKey", "primitiveValue"));
                      }
                    }));
          }

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("compositeKey", "compositeValue"));
          }
        };

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(myTransform);

    assertThat(displayData, not(hasItem(hasDisplayItem("compositeKey", "compositeValue"))));
    assertThat(displayData, hasItem(hasDisplayItem("primitiveKey", "primitiveValue")));
  }

  @Test
  public void testPrimitiveTransform() {
    PTransform<? super PCollection<Integer>, ? super PCollection<Integer>> myTransform =
        ParDo.of(
            new DoFn<Integer, Integer>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {}

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.add(DisplayData.item("foo", "bar"));
              }
            });

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(myTransform);

    assertThat(displayData, hasItem(hasDisplayItem("foo")));
  }

  @Test
  public void testSourceTransform() {
    PTransform<? super PBegin, ? extends POutput> myTransform = TextIO.read().from("foo.*");

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(myTransform);

    assertThat(displayData, hasItem(hasDisplayItem("filePattern", "foo.*")));
  }
}
