/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.construction.classloader;

import static java.util.Collections.list;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.stream.Stream;

/**
 * inspired from xbean, ensures the portability of the "scanning" and the impl independent logic.
 */
public final class Classloaders {
  private static final ClassLoader SYSTEM = ClassLoader.getSystemClassLoader();

  public static Stream<File> toFiles(final ClassLoader classLoader) throws IOException {
    if (classLoader == null) {
      return Stream.empty();
    }
    return Stream.concat(
        list(classLoader.getResources("")).stream(),
        list(classLoader.getResources("META-INF")).stream()
            .map(url -> {
              final String externalForm = url.toExternalForm();
              try {
                return new URL(externalForm.substring(0, externalForm.lastIndexOf("META-INF")));
              } catch (final MalformedURLException e) {
                throw new IllegalArgumentException(e);
              }
            }))
        .map(Classloaders::toFile);
  }

  private static boolean isSystemParent(final ClassLoader classLoader) {
    ClassLoader current = SYSTEM.getParent();
    while (current != null) {
      if (current == classLoader) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  private static File toFile(final URL url) {
    if ("jar".equals(url.getProtocol())) {
      try {
        final String spec = url.getFile();
        final int separator = spec.indexOf('!');
        if (separator == -1) {
          return null;
        }
        return toFile(new URL(spec.substring(0, separator + 1)));
      } catch (final MalformedURLException e) {
        // let it fail
      }
    } else if ("file".equals(url.getProtocol())) {
      String path = decode(url.getFile());
      if (path.endsWith("!")) {
        path = path.substring(0, path.length() - 1);
      }
      return new File(path);
    }
    throw new IllegalArgumentException("Unsupported entry: " + url.toExternalForm());
  }

  private static String decode(String fileName) {
    if (fileName.indexOf('%') == -1) {
      return fileName;
    }

    final StringBuilder result = new StringBuilder(fileName.length());
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < fileName.length(); ) {
      final char c = fileName.charAt(i);
      if (c == '%') {
        out.reset();
        do {
          if (i + 2 >= fileName.length()) {
            throw new IllegalArgumentException("Incomplete % sequence at: " + i);
          }
          final int d1 = Character.digit(fileName.charAt(i + 1), 16);
          final int d2 = Character.digit(fileName.charAt(i + 2), 16);
          if (d1 == -1 || d2 == -1) {
            throw new IllegalArgumentException("Invalid % sequence ("
                    + fileName.substring(i, i + 3) + ") at: " + String.valueOf(i));
          }
          out.write((byte) ((d1 << 4) + d2));
          i += 3;
        } while (i < fileName.length() && fileName.charAt(i) == '%');
        result.append(out.toString());
      } else {
        result.append(c);
        i++;
      }
    }
    return result.toString();
  }

  private Classloaders() {
    // no-op
  }
}
