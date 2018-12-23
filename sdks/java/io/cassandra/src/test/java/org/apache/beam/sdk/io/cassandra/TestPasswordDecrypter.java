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
package org.apache.beam.sdk.io.cassandra;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** Class used in the tests to decrypt the Cassandra password. */
public class TestPasswordDecrypter implements PasswordDecrypter {

  /**
   * Can't use the Mockito.verify() method to inspect the passwordDecrypter.decrypt() method
   * invocation in integration tests because the PasswordDecrypter instance is directly deserialized
   * on workers. Example => verify(passwordDecrypter, atLeast(1)).decrypt(ENCRYPTED_PASSWORD);
   */
  private static final Map<String, Long> nbCallsBySession = new HashMap<>();

  private final String sessionUID;

  public TestPasswordDecrypter(String sessionUID) {
    this.sessionUID = sessionUID;
  }

  @Override
  public String decrypt(String encryptedPassword) {
    nbCallsBySession.put(sessionUID, nbCallsBySession.getOrDefault(sessionUID, 0L) + 1L);

    return new String(Base64.getDecoder().decode(encryptedPassword), StandardCharsets.UTF_8);
  }

  public static long getNbCallsBySession(String sessionUID) {
    return nbCallsBySession.get(sessionUID);
  }
}
