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

import static java.util.Arrays.asList;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.fromBeamCoder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of the wrapping of Beam Coders as Spark ExpressionEncoders. */
@RunWith(JUnit4.class)
public class EncoderHelpersTest {

  @ClassRule public static SparkSessionRule sessionRule = new SparkSessionRule();

  private <T> Dataset<T> createDataset(List<T> data, Encoder<T> encoder) {
    Dataset<T> ds = sessionRule.getSession().createDataset(data, encoder);
    ds.printSchema();
    return ds;
  }

  @Test
  public void beamCoderToSparkEncoderTest() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> dataset = createDataset(data, EncoderHelpers.fromBeamCoder(VarIntCoder.of()));
    assertEquals(data, dataset.collectAsList());
  }

  @Test
  public void testBeamEncoderOfPrivateType() {
    // Verify concrete types are not used in coder generation.
    // In case of private types this would cause an IllegalAccessError.
    List<PrivateString> data = asList(new PrivateString("1"), new PrivateString("2"));
    Dataset<PrivateString> dataset = createDataset(data, fromBeamCoder(PrivateString.CODER));
    assertThat(dataset.collect(), equalTo(data.toArray()));
  }

  private static class PrivateString {
    private static final Coder<PrivateString> CODER =
        DelegateCoder.of(
            StringUtf8Coder.of(),
            str -> str.string,
            PrivateString::new,
            new TypeDescriptor<PrivateString>() {});

    private final String string;

    public PrivateString(String string) {
      this.string = string;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrivateString that = (PrivateString) o;
      return Objects.equals(string, that.string);
    }

    @Override
    public int hashCode() {
      return Objects.hash(string);
    }
  }
}
