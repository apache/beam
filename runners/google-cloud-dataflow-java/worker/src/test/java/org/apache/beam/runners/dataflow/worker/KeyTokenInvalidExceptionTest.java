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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KeyTokenInvalidException}. */
@RunWith(JUnit4.class)
public final class KeyTokenInvalidExceptionTest {
  @Test
  public void testIsKeyTokenInvalidException() throws Exception {
    KeyTokenInvalidException exception = new KeyTokenInvalidException("test");
    RuntimeException keyTokenCauseException = new RuntimeException("key token cause", exception);
    assertTrue(KeyTokenInvalidException.isKeyTokenInvalidException(exception));
    assertTrue(KeyTokenInvalidException.isKeyTokenInvalidException(keyTokenCauseException));
    assertFalse(
        KeyTokenInvalidException.isKeyTokenInvalidException(new RuntimeException("non key token")));
  }
}
