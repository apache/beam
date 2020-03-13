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
package org.apache.beam.sdk.testutils.jvmverification;

import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v10;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v11;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v12;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v13;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v14;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_1;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_2;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_3;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_4;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_5;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_6;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_7;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_8;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v9;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

public class JvmVerification {

  private static final Map<String, Java> versionMapping = new HashMap<>();

  static {
    versionMapping.put("002D", v1_1);
    versionMapping.put("002E", v1_2);
    versionMapping.put("002F", v1_3);
    versionMapping.put("0030", v1_4);
    versionMapping.put("0031", v1_5);
    versionMapping.put("0032", v1_6);
    versionMapping.put("0033", v1_7);
    versionMapping.put("0034", v1_8);
    versionMapping.put("0035", v9);
    versionMapping.put("0036", v10);
    versionMapping.put("0037", v11);
    versionMapping.put("0038", v12);
    versionMapping.put("0039", v13);
    versionMapping.put("003A", v14);
  }

  // bytecode
  @Test
  public void verifyCodeIsCompiledWithJava8() throws IOException {
    assertEquals(v1_8, getByteCodeVersion(DoFn.class));
  }

  @Test
  public void verifyTestCodeIsCompiledWithJava8() throws IOException {
    assertEquals(v1_8, getByteCodeVersion(JvmVerification.class));
  }

  // jvm
  @Test
  public void verifyRunningJVMVersionIs11() {
    final String version = getJavaSpecification();
    assertEquals(v11.name, version);
  }

  private static <T> Java getByteCodeVersion(final Class<T> clazz) throws IOException {
    final InputStream stream =
        clazz.getClassLoader().getResourceAsStream(clazz.getName().replace(".", "/") + ".class");
    final byte[] classBytes = IOUtils.toByteArray(stream);
    final String versionInHexString =
        Hex.encodeHexString(new byte[] {classBytes[6], classBytes[7]});
    return versionMapping.get(versionInHexString);
  }

  private static String getJavaSpecification() {
    return System.getProperty("java.specification.version");
  }

  enum Java {
    v1_1("1.1"),
    v1_2("1.2"),
    v1_3("1.3"),
    v1_4("1.4"),
    v1_5("1.5"),
    v1_6("1.6"),
    v1_7("1.7"),
    v1_8("1.8"),
    v9("9"),
    v10("10"),
    v11("11"),
    v12("12"),
    v13("13"),
    v14("14");

    final String name;

    Java(final String name) {
      this.name = name;
    }
  }
}
