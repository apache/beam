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
package org.apache.beam.sdk.io.snowflake.test;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  private static final String PRIVATE_KEY_FILE_NAME = "test_rsa_key.p8";
  private static final String PRIVATE_KEY_PASSPHRASE = "snowflake";

  public static String getPrivateKeyPath(Class klass) {
    ClassLoader classLoader = klass.getClassLoader();
    File file = new File(classLoader.getResource(PRIVATE_KEY_FILE_NAME).getFile());
    return file.getAbsolutePath();
  }

  public static String getPrivateKeyPassphrase() {
    return PRIVATE_KEY_PASSPHRASE;
  }
}
