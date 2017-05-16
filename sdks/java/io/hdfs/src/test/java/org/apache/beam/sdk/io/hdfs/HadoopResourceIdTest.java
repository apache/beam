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
package org.apache.beam.sdk.io.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link HadoopResourceId}.
 */
@RunWith(JUnit4.class)
public class HadoopResourceIdTest {
  private static final String DIR = "hdfs://localhost:999/dir/";
  private static final String FILE = "hdfs://localhost:999/dir/file";

  @Test
  public void testEquals() {
    assertEquals(new HadoopResourceId(URI.create(DIR)),
        new HadoopResourceId(URI.create(DIR)));
    assertNotEquals(new HadoopResourceId(URI.create(DIR)),
        new HadoopResourceId(URI.create(FILE)));
  }

  @Test
  public void testIsDirectory() {
    assertTrue(new HadoopResourceId(URI.create(DIR)).isDirectory());
    assertFalse(new HadoopResourceId(URI.create(FILE)).isDirectory());
  }

  @Test
  public void testToString() {
    assertEquals(DIR, new HadoopResourceId(URI.create(DIR)).toString());
    assertEquals(FILE, new HadoopResourceId(URI.create(FILE)).toString());
  }

  @Test
  public void testFilename() {
    assertEquals("file", new HadoopResourceId(URI.create(FILE)).getFilename());
  }

  @Test
  public void testScheme() {
    assertEquals("hdfs", new HadoopResourceId(URI.create(FILE)).getScheme());
  }

  @Test
  public void testGetCurrentDirectory() {
    assertEquals(DIR, new HadoopResourceId(URI.create(DIR)).getCurrentDirectory().toString());
    assertEquals(DIR, new HadoopResourceId(URI.create(FILE)).getCurrentDirectory().toString());
  }
}
