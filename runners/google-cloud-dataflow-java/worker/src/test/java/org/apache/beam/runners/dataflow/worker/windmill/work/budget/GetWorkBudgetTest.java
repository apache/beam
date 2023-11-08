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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetWorkBudgetTest {

  @Test
  public void testCreateWithNoBudget() {
    GetWorkBudget getWorkBudget = GetWorkBudget.noBudget();
    assertEquals(0, getWorkBudget.items());
    assertEquals(0, getWorkBudget.bytes());
  }

  @Test
  public void testBuild_itemsAndBytesNeverBelowZero() {
    GetWorkBudget getWorkBudget = GetWorkBudget.builder().setItems(-10).setBytes(-10).build();
    assertEquals(0, getWorkBudget.items());
    assertEquals(0, getWorkBudget.bytes());
  }

  @Test
  public void testAdd_doesNotAllowNegativeParameters() {
    GetWorkBudget getWorkBudget = GetWorkBudget.builder().setItems(1).setBytes(1).build();
    assertThrows(IllegalArgumentException.class, () -> getWorkBudget.add(-1, -1));
  }

  @Test
  public void testSubtract_itemsAndBytesNeverBelowZero() {
    GetWorkBudget getWorkBudget = GetWorkBudget.builder().setItems(1).setBytes(1).build();
    GetWorkBudget subtracted = getWorkBudget.subtract(10, 10);
    assertEquals(0, subtracted.items());
    assertEquals(0, subtracted.bytes());
  }

  @Test
  public void testSubtractGetWorkBudget_itemsAndBytesNeverBelowZero() {
    GetWorkBudget getWorkBudget = GetWorkBudget.builder().setItems(1).setBytes(1).build();
    GetWorkBudget subtracted =
        getWorkBudget.subtract(GetWorkBudget.builder().setItems(10).setBytes(10).build());
    assertEquals(0, subtracted.items());
    assertEquals(0, subtracted.bytes());
  }

  @Test
  public void testSubtract_doesNotAllowNegativeParameters() {
    GetWorkBudget getWorkBudget = GetWorkBudget.builder().setItems(1).setBytes(1).build();
    assertThrows(IllegalArgumentException.class, () -> getWorkBudget.subtract(-1, -1));
  }
}
