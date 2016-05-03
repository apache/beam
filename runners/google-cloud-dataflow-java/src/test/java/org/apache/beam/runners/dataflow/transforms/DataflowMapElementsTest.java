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
package org.apache.beam.runners.dataflow.transforms;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;

import org.junit.Test;

import java.io.Serializable;
import java.util.Set;

/**
 * Unit tests for Dataflow usage of {@link MapElements} transforms.
 */
public class DataflowMapElementsTest implements Serializable {
  @Test
  public void testPrimitiveDisplayData() {
    SimpleFunction<?, ?> mapFn = new SimpleFunction<Integer, Integer>() {
      @Override
      public Integer apply(Integer input) {
        return input;
      }
    };

    MapElements<?, ?> map = MapElements.via(mapFn);
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(map);
    assertThat("MapElements should include the mapFn in its primitive display data",
        displayData, hasItem(hasDisplayItem("mapFn", mapFn.getClass())));
  }
}
