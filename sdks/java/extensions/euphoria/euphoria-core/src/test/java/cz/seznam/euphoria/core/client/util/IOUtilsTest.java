/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.seznam.euphoria.core.util.IOUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.Test;

public class IOUtilsTest {

  @Test(expected = IOException.class)
  public void testOneIOException() throws IOException {
    IOUtils.forEach(
        Arrays.asList(1, 2, 3),
        (i) -> {
          if (i == 2) {
            throw new IOException("Number: " + i);
          }
        });
  }

  @Test
  public void testSuppressedIOException() throws IOException {
    try {
      IOUtils.forEach(
          Arrays.asList(1, 2, 3),
          (i) -> {
            throw new IOException("Number: " + i);
          });
    } catch (Exception e) {
      assertEquals(2, e.getSuppressed().length); // two supressed exceptions and one thrown
      assertTrue(e instanceof IOException);
      assertEquals("Number: 1", e.getMessage());
    }
  }

  @Test(expected = IOException.class)
  public void testStreamIOException() throws IOException {

    IOUtils.forEach(
        Stream.of(1, 2, 3),
        (i) -> {
          if (i == 2) {
            throw new IOException("Number: " + i);
          }
        });
  }
}
