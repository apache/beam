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

import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v11;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v17;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v1_8;
import static org.apache.beam.sdk.testutils.jvmverification.JvmVerification.Java.v21;
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
    versionMapping.put("0034", v1_8);
    versionMapping.put("0037", v11);
    versionMapping.put("003d", v17);
    versionMapping.put("0041", v21);
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

  @Test
  public void verifyTestCodeIsCompiledWithJava11() throws IOException {
    assertEquals(v11, getByteCodeVersion(JvmVerification.class));
  }

  @Test
  public void verifyTestCodeIsCompiledWithJava17() throws IOException {
    assertEquals(v17, getByteCodeVersion(JvmVerification.class));
  }

  @Test
  public void verifyTestCodeIsCompiledWithJava21() throws IOException {
    assertEquals(v21, getByteCodeVersion(JvmVerification.class));
  }

  // jvm
  @Test
  public void verifyRunningJVMVersionIs11() {
    final String version = getJavaSpecification();
    assertEquals(v11.name, version);
  }

  @Test
  public void verifyRunningJVMVersionIs17() {
    final String version = getJavaSpecification();
    assertEquals(v17.name, version);
  }

  @Test
  public void verifyRunningJVMVersionIs21() {
    final String version = getJavaSpecification();
    assertEquals(v21.name, version);
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
    v1_8("1.8"),
    v11("11"),
    v17("17"),
    v21("21");

    final String name;

    Java(final String name) {
      this.name = name;
    }
  }
}
