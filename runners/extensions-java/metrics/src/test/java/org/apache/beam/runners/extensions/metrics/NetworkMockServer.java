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
package org.apache.beam.runners.extensions.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** Mock of a network server. */
class NetworkMockServer {
  private final int port;
  private ServerSocket serverSocket;
  private ServerThread thread;
  private CountDownLatch countDownLatch;

  private Collection<String> messages = new CopyOnWriteArrayList<String>();

  public NetworkMockServer(final int port) {
    this.port = port;
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  public NetworkMockServer start() throws IOException {
    serverSocket = new ServerSocket(port);
    thread = new ServerThread(serverSocket, messages);
    thread.start();
    return this;
  }

  public void stop() throws IOException {
    thread.shutdown();
    serverSocket.close();
  }

  public Collection<String> getMessages() {
    return messages;
  }

  public void clear() {
    messages.clear();
  }

  private class ServerThread extends Thread {
    private final Collection<String> messages;

    private final AtomicBoolean done = new AtomicBoolean(false);
    private final ServerSocket server;

    public ServerThread(final ServerSocket server, final Collection<String> messages) {
      this.messages = messages;
      this.server = server;
      setName("network-mock-server");
    }

    @Override
    public void run() {
      while (!done.get()) {
        try {
          final Socket s = server.accept();
          synchronized (this) {
            final InputStream is = s.getInputStream();
            final BufferedReader reader =
                new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            String line;

            try {
              while ((line = reader.readLine()) != null) {
                messages.add(line);
              }
            } finally {
              countDownLatch.countDown();
              s.close();
            }
          }
        } catch (final IOException e) {
          if (!done.get()) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    public void shutdown() {
      done.set(true);
    }
  }
}
