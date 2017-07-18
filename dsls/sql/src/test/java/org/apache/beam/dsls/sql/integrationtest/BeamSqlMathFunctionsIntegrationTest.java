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
package org.apache.beam.dsls.sql.integrationtest;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.dsls.sql.BeamSqlDslBase;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for built-in Math functions.
 */
public class BeamSqlMathFunctionsIntegrationTest extends BeamSqlDslBase implements Serializable {

  @Test
  public void testRandRandInteger() throws Exception {
    String sql = "SELECT RAND(f_int) as a, RAND(100) as b, RAND() as c, "
        + "RAND_INTEGER(10) as d, RAND_INTEGER(10, 100) as e "
        + "FROM PCOLLECTION";

    PCollection<BeamSqlRow> result = boundedInput2
        .apply("testRandRandInteger", BeamSql.simpleQuery(sql));

    PAssert.that(result).satisfies(new SerializableFunction<Iterable<BeamSqlRow>, Void>() {
      @Override
      public Void apply(Iterable<BeamSqlRow> input) {
        Iterator<BeamSqlRow> ite = input.iterator();
        Assert.assertTrue(ite.hasNext());
        BeamSqlRow row = ite.next();

        Assert.assertEquals(new Random(1).nextDouble(), row.getDouble(0), 0);
        Assert.assertEquals(new Random(100).nextDouble(), row.getDouble(1), 0);
        Assert.assertTrue(row.getDouble(2) >= 0 && row.getDouble(2) < 1);

        Assert.assertTrue(row.getInteger(3) >= 0 && row.getInteger(3) < 10);
        Assert.assertEquals(new Random(10).nextInt(100), row.getInteger(4));

        Assert.assertFalse(ite.hasNext());
        return null;
      }
    });

    pipeline.run().waitUntilFinish();
  }
}
