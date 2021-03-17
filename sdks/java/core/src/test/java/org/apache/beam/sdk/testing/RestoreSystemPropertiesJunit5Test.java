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
package org.apache.beam.sdk.testing;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({RestoreSystemPropertiesJunit5.class})
public class RestoreSystemPropertiesJunit5Test {
  /*
   * Since these tests can run out of order, both test A and B verify that they
   * could insert their property and that the other does not exist.
   */
  @Test
  public void testThatPropertyIsClearedA() {
    System.getProperties().put("TestA", "TestA");
    assertNotNull(System.getProperty("TestA"));
    assertNull(System.getProperty("TestB"));
  }

  @Test
  public void testThatPropertyIsClearedB() {
    System.getProperties().put("TestB", "TestB");
    assertNotNull(System.getProperty("TestB"));
    assertNull(System.getProperty("TestA"));
  }
}
