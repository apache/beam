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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link UnownedInputStream}. */
@RunWith(JUnit4.class)
public class UnownedInputStreamTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  private ByteArrayInputStream bais;
  private UnownedInputStream os;

  @Before
  public void setup() {
    bais = new ByteArrayInputStream(new byte[] {1, 2, 3});
    os = new UnownedInputStream(bais);
  }

  @Test
  public void testHashCodeEqualsAndToString() throws Exception {
    assertEquals(bais.hashCode(), os.hashCode());
    assertEquals("UnownedInputStream{in=" + bais + "}", os.toString());
    assertEquals(new UnownedInputStream(bais), os);
  }

  @Test
  public void testClosingThrows() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    expectedException.expectMessage("close()");
    os.close();
  }

  @Test
  public void testMarkThrows() throws Exception {
    assertFalse(os.markSupported());
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    expectedException.expectMessage("mark()");
    os.mark(1);
  }

  @Test
  public void testResetThrows() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    expectedException.expectMessage("reset()");
    os.reset();
  }
}
