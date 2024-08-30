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
package org.apache.beam.io.requestresponse;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CallShouldBackoffBasedOnRejectionProbability}. */
@RunWith(JUnit4.class)
public class CallShouldBackoffBasedOnRejectionProbabilityTest {

  @Test
  public void testValue() {
    for (Case caze : CASES) {
      CallShouldBackoffBasedOnRejectionProbability<String> shouldBackoff = instance();
      for (boolean ar : caze.acceptRejects) {
        if (ar) {
          shouldBackoff.update("");
        } else {
          shouldBackoff.update(new UserCodeExecutionException(""));
        }
      }
      assertEquals(caze.toString(), caze.wantPReject, shouldBackoff.getRejectionProbability(), 0.1);
      assertEquals(caze.toString(), caze.wantValue, shouldBackoff.isTrue());
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          of(0, false),
          of(0, false, true, true, true, true, true, true, true, true, true, true, true),
          of(0, false, true),
          of(0.5, false, false),
          of(0.91, true, false, false, false, false, false, false, false, false, false, false));

  private static Case of(double wantPReject, boolean wantValue, boolean... acceptRejects) {
    List<Boolean> list = new ArrayList<>();
    for (boolean ar : acceptRejects) {
      list.add(ar);
    }
    return new Case(list, wantPReject, wantValue);
  }

  private static class Case {
    private final List<Boolean> acceptRejects;
    private final double wantPReject;
    private final boolean wantValue;

    Case(List<Boolean> acceptRejects, double wantPReject, boolean wantValue) {
      this.acceptRejects = acceptRejects;
      this.wantPReject = wantPReject;
      this.wantValue = wantValue;
    }

    @Override
    public String toString() {
      return "Case{"
          + "acceptRejects="
          + acceptRejects
          + ", wantPReject="
          + wantPReject
          + ", wantValue="
          + wantValue
          + '}';
    }
  }

  CallShouldBackoffBasedOnRejectionProbability<String> instance() {
    return new CallShouldBackoffBasedOnRejectionProbability<String>().setThreshold(0.5);
  }
}
