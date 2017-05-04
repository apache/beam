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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for OldDoFn.
 */
@RunWith(JUnit4.class)
public class OldDoFnTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPopulateDisplayDataDefaultBehavior() {
    OldDoFn<String, String> usesDefault =
        new OldDoFn<String, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {}
        };

    DisplayData data = DisplayData.from(usesDefault);
    assertThat(data.items(), empty());
  }
}
