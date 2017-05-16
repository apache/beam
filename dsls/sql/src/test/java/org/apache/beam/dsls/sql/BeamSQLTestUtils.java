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

package org.apache.beam.dsls.sql;

import static junit.framework.TestCase.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.beam.dsls.sql.schema.BeamSQLRow;

/**
 * Utility methods for testing BEAM SQL.
 */
public class BeamSQLTestUtils {
  /**
   * assert two collections of {@code BeamSQLRow} are equals ignore order.
   * @param rows1
   * @param rows2
   */
  public static void assertEqualsIgnoreOrder(Collection<BeamSQLRow> rows1,
      Collection<BeamSQLRow> rows2) {
    Collection<String> strRows1 = new ArrayList<>(rows1.size());
    for (BeamSQLRow row : rows1) {
      strRows1.add(row.valueInString());
    }

    Collection<String> strRows2 = new ArrayList<>(rows2.size());
    for (BeamSQLRow row : rows2) {
      strRows2.add(row.valueInString());
    }

    Iterator<String> itr = strRows1.iterator();
    boolean eq = true;
    while (itr.hasNext()) {
      String row = itr.next();
      if (!strRows2.contains(row)) {
        eq = false;
        break;
      }
    }
    if (!eq) {
      System.err.println("Expected:");
      for (BeamSQLRow exp : rows1) {
        System.err.println(exp.valueInString());
      }
      System.err.println("Actual:");
      for (BeamSQLRow exp : rows2) {
        System.err.println(exp.valueInString());
      }
    }
    assertTrue(eq);
  }
}
