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
package org.apache.beam.sdk.io.expansion;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that inspect dependency at runtime, when Gradle dependency query isn't sufficient to locate
 * offending dependencies.
 */
@RunWith(JUnit4.class)
@Ignore("These tests are run manually by maintainers to help inspect dependencies")
public class ExpansionServiceTest {
  private static final Logger LOG = LoggerFactory.getLogger(ExpansionServiceTest.class);

  /**
   * Check which jar a class comes from at runtime. This gives the actual class resolution at
   * runtime in the case of same classes appeared in different dependencies.
   */
  @Test
  public void testClassJar() throws ClassNotFoundException {
    //
    String className = "net.minidev.json.JSONArray";
    Class<?> forName = Class.forName(className);
    LOG.info("Class found in jar {}", forName.getProtectionDomain().getCodeSource().getLocation());
  }

  /**
   * Check where a resource comes from.
   *
   * <p>Example use case: when there is a new vulnerability reported but not found by Gradle
   * dependency query. It's likely such report is a false positive due to maven metadata files
   * detected by scanner, but the actual class is different (can be confirmed by {@code
   * testClassJar} above.
   */
  @Test
  public void testMetaInf() throws IOException {
    String resourcePath = "/META-INF/maven/net.minidev/json-smart/pom.xml";
    URL resourceUrl = ExpansionServiceTest.class.getResource(resourcePath);
    if (resourceUrl != null) {
      if (resourceUrl.getProtocol().equals("jar")) {
        JarURLConnection jarConnection = (JarURLConnection) resourceUrl.openConnection();
        String jarFilePath = jarConnection.getJarFileURL().getFile();
        LOG.info("Resource found in JAR: {}", jarFilePath);
      } else if (resourceUrl.getProtocol().equals("file")) {
        LOG.info("Resource found in file system: {}", resourceUrl.getFile());
      } else {
        LOG.info("Resource found at: {}", resourceUrl);
      }
    } else {
      LOG.info("Resource not found: {}", resourcePath);
    }
  }
}
