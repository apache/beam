/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;

/** Unit tests for {@link UnownedOutputStream}. */
@RunWith(JUnit4.class)
public class UnownedOutputStreamTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  private ByteArrayOutputStream baos;
  private UnownedOutputStream os;

  @Before
  public void setup() {
    baos = new ByteArrayOutputStream();
    os = new UnownedOutputStream(baos);
  }

  @Test
  public void testHashCodeEqualsAndToString() throws Exception {
    assertEquals(baos.hashCode(), os.hashCode());
    assertEquals("UnownedOutputStream{out=" + baos + "}", os.toString());
    assertEquals(new UnownedOutputStream(baos), os);
  }

  @Test
  public void testClosingThrows() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    os.close();
  }
}

