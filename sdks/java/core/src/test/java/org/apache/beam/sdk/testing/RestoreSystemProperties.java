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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/** Saves and restores the current system properties for tests. */
public class RestoreSystemProperties extends ExternalResource implements TestRule {
  private byte[] originalProperties;

  @Override
  protected void before() throws Throwable {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.getProperties().store(baos, "");
    baos.close();
    originalProperties = baos.toByteArray();
  }

  @Override
  protected void after() {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(originalProperties)) {
      System.getProperties().clear();
      System.getProperties().load(bais);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
