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
package org.apache.beam.sdk.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;

/**
 * A class that creates UUID version 3 based on MAC address, secure random bytes and current
 * timestamp.
 */
public final class UniqueIdGenerator {
  public static UUID getUniqueId() {
    SecureRandom secureRandom = new SecureRandom();
    byte[] randomBytes = new byte[16];
    secureRandom.nextBytes(randomBytes);
    byte[] timeBytes = Longs.toByteArray(System.currentTimeMillis());
    List<byte[]> addressBytesList = getNetworkInterfaceHardwareAddresses();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      outputStream.write(randomBytes);
      outputStream.write(timeBytes);
      for (byte[] addressBytes : addressBytesList) {
        outputStream.write(addressBytes);
      }
    } catch (IOException e) {
      return UUID.randomUUID();
    }
    return UUID.nameUUIDFromBytes(outputStream.toByteArray());
  }

  private static List<byte[]> getNetworkInterfaceHardwareAddresses() {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
      while (interfaces.hasMoreElements()) {
        byte[] hardwareAddress = interfaces.nextElement().getHardwareAddress();
        if (hardwareAddress != null) {
          builder.add(hardwareAddress);
        }
      }
      return builder.build();
    } catch (SocketException e) {
      return ImmutableList.of();
    }
  }
}
