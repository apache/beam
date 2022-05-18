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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.spark.sql.Dataset;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of the wrapping of Beam Coders as Spark ExpressionEncoders. */
@RunWith(JUnit4.class)
public class EncoderHelpersTest {

  @ClassRule public static SparkSessionRule sessionRule = new SparkSessionRule();

  @Test
  public void beamCoderToSparkEncoderTest() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> dataset =
        sessionRule
            .getSession()
            .createDataset(data, EncoderHelpers.fromBeamCoder(VarIntCoder.of()));
    assertEquals(data, dataset.collectAsList());
  }
}
