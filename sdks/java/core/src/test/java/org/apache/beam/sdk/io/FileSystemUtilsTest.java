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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSystemUtilsTest {

  @Test
  public void testGlobTranslation() {
    assertEquals("foo", wildcardToRegexp("foo"));
    assertEquals("fo[^/]*o", wildcardToRegexp("fo*o"));
    assertEquals("f[^/]*o\\.[^/]", wildcardToRegexp("f*o.?"));
    assertEquals("foo-[0-9][^/]*", wildcardToRegexp("foo-[0-9]*"));
    assertEquals("foo-[0-9].*", wildcardToRegexp("foo-[0-9]**"));
    assertEquals(".*foo", wildcardToRegexp("**/*foo"));
    assertEquals(".*foo", wildcardToRegexp("**foo"));
    assertEquals("foo/[^/]*", wildcardToRegexp("foo/*"));
    assertEquals("foo[^/]*", wildcardToRegexp("foo*"));
    assertEquals("foo/[^/]*/[^/]*/[^/]*", wildcardToRegexp("foo/*/*/*"));
    assertEquals("foo/[^/]*/.*", wildcardToRegexp("foo/*/**"));
    assertEquals("foo.*baz", wildcardToRegexp("foo**baz"));
    assertEquals("foo\\", wildcardToRegexp("foo\\"));
    assertEquals("foo/bar\\baz[^/]*", wildcardToRegexp("foo/bar\\baz*"));
  }
}
