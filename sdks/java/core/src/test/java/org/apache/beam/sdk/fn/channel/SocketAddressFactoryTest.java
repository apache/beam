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
package org.apache.beam.sdk.fn.channel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.unix.DomainSocketAddress;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SocketAddressFactory}. */
@RunWith(JUnit4.class)
public class SocketAddressFactoryTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testHostPortSocket() {
    SocketAddress socketAddress = SocketAddressFactory.createFrom("localhost:123");
    assertThat(socketAddress, Matchers.instanceOf(InetSocketAddress.class));
    assertEquals("localhost", ((InetSocketAddress) socketAddress).getHostString());
    assertEquals(123, ((InetSocketAddress) socketAddress).getPort());
  }

  @Test
  public void testDomainSocket() throws Exception {
    File tmpFile = tmpFolder.newFile();
    SocketAddress socketAddress =
        SocketAddressFactory.createFrom("unix://" + tmpFile.getAbsolutePath());
    assertThat(socketAddress, Matchers.instanceOf(DomainSocketAddress.class));
    assertEquals(tmpFile.getAbsolutePath(), ((DomainSocketAddress) socketAddress).path());
  }
}
