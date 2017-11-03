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

package org.apache.beam.harness.channel;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.net.HostAndPort;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/** Creates a {@link SocketAddress} based upon a supplied string. */
public class SocketAddressFactory {
  private static final String UNIX_DOMAIN_SOCKET_PREFIX = "unix://";

  /**
   * Parse a {@link SocketAddress} from the given string.
   */
  public static SocketAddress createFrom(String value) {
    if (value.startsWith(UNIX_DOMAIN_SOCKET_PREFIX)) {
      // Unix Domain Socket address.
      // Create the underlying file for the Unix Domain Socket.
      String filePath = value.substring(UNIX_DOMAIN_SOCKET_PREFIX.length());
      File file = new File(filePath);
      if (!file.isAbsolute()) {
        throw new IllegalArgumentException("File path must be absolute: " + filePath);
      }
      try {
        if (file.createNewFile()) {
          // If this application created the file, delete it when the application exits.
          file.deleteOnExit();
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      // Create the SocketAddress referencing the file.
      return new DomainSocketAddress(file);
    } else {
      // Standard TCP/IP address.
      HostAndPort hostAndPort = HostAndPort.fromString(value);
      checkArgument(hostAndPort.hasPort(),
          "Address must be a unix:// path or be in the form host:port. Got: %s", value);
      return new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
    }
  }
}
