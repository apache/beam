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
package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for functions in {@link IOITHelper}. */
@RunWith(JUnit4.class)
public class IOITHelperTest {
  private static long startTimeMeasure;
  private static String message = "";
  private static ArrayList<Exception> listOfExceptionsThrown;

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setup() {
    listOfExceptionsThrown = new ArrayList<>();
  }

  @Test
  public void retryHealthyFunction() throws Exception {
    executeWithRetry(IOITHelperTest::validFunction);
    assertEquals("The healthy function.", message);
  }

  @Test
  public void retryFunctionThatWillFail() throws Exception {
    exceptionRule.expect(SQLException.class);
    exceptionRule.expectMessage("Problem with connection");
    executeWithRetry(IOITHelperTest::failingFunction);
    assertEquals(3, listOfExceptionsThrown.size());
  }

  @Test
  public void retryFunctionThatFailsWithMoreAttempts() throws Exception {
    exceptionRule.expect(SQLException.class);
    exceptionRule.expectMessage("Problem with connection");
    executeWithRetry(4, 1_000, IOITHelperTest::failingFunction);
    assertEquals(4, listOfExceptionsThrown.size());
  }

  @Test
  public void retryFunctionThatRecovers() throws Exception {
    startTimeMeasure = System.currentTimeMillis();
    executeWithRetry(IOITHelperTest::recoveringFunction);
    assertEquals(1, listOfExceptionsThrown.size());
  }

  @Test
  public void retryFunctionThatRecoversAfterBiggerDelay() throws Exception {
    startTimeMeasure = System.currentTimeMillis();
    executeWithRetry(3, 2_000, IOITHelperTest::recoveringFunctionWithBiggerDelay);
    assertEquals(1, listOfExceptionsThrown.size());
  }

  private static void failingFunction() throws SQLException {
    SQLException e = new SQLException("Problem with connection");
    listOfExceptionsThrown.add(e);
    throw e;
  }

  private static void recoveringFunction() throws SQLException {
    if (System.currentTimeMillis() - startTimeMeasure < 1_001) {
      SQLException e = new SQLException("Problem with connection");
      listOfExceptionsThrown.add(e);
      throw e;
    }
  }

  private static void recoveringFunctionWithBiggerDelay() throws SQLException {
    if (System.currentTimeMillis() - startTimeMeasure < 2_001) {
      SQLException e = new SQLException("Problem with connection");
      listOfExceptionsThrown.add(e);
      throw e;
    }
  }

  private static void validFunction() throws SQLException {
    message = "The healthy function.";
  }
}
