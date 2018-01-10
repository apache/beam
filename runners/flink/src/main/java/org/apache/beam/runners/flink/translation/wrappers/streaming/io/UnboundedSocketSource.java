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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.io.UnboundedSource.CheckpointMark.NOOP_CHECKPOINT_MARK;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example unbounded Beam source that reads input from a socket.
 * This is used mainly for testing and debugging.
 * */
public class UnboundedSocketSource<CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends UnboundedSource<String, CheckpointMarkT> {

  private static final Coder<String> DEFAULT_SOCKET_CODER = StringUtf8Coder.of();

  private static final long serialVersionUID = 1L;

  private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

  private static final int CONNECTION_TIMEOUT_TIME = 0;

  private final String hostname;
  private final int port;
  private final char delimiter;
  private final long maxNumRetries;
  private final long delayBetweenRetries;

  public UnboundedSocketSource(String hostname, int port, char delimiter, long maxNumRetries) {
    this(hostname, port, delimiter, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
  }

  public UnboundedSocketSource(String hostname,
                               int port,
                               char delimiter,
                               long maxNumRetries,
                               long delayBetweenRetries) {
    this.hostname = hostname;
    this.port = port;
    this.delimiter = delimiter;
    this.maxNumRetries = maxNumRetries;
    this.delayBetweenRetries = delayBetweenRetries;
  }

  public String getHostname() {
    return this.hostname;
  }

  public int getPort() {
    return this.port;
  }

  public char getDelimiter() {
    return this.delimiter;
  }

  public long getMaxNumRetries() {
    return this.maxNumRetries;
  }

  public long getDelayBetweenRetries() {
    return this.delayBetweenRetries;
  }

  @Override
  public List<? extends UnboundedSource<String, CheckpointMarkT>> split(
      int desiredNumSplits,
      PipelineOptions options) throws Exception {
    return Collections.<UnboundedSource<String, CheckpointMarkT>>singletonList(this);
  }

  @Override
  public UnboundedReader<String> createReader(PipelineOptions options,
                                              @Nullable CheckpointMarkT checkpointMark) {
    return new UnboundedSocketReader(this);
  }

  @Nullable
  @Override
  public Coder getCheckpointMarkCoder() {
    // Flink and Dataflow have different checkpointing mechanisms.
    // In our case we do not need a coder.
    return null;
  }

  @Override
  public void validate() {
    checkArgument(port > 0 && port < 65536, "port is out of range");
    checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), "
        + "or -1 (infinite retries)");
    checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");
  }

  @Override
  public Coder<String> getOutputCoder() {
    return DEFAULT_SOCKET_CODER;
  }

  /**
   * Unbounded socket reader.
   */
  public static class UnboundedSocketReader extends UnboundedSource.UnboundedReader<String> {

    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSocketReader.class);

    private final UnboundedSocketSource source;

    private Socket socket;
    private BufferedReader reader;

    private boolean isRunning;

    private String currentRecord;

    public UnboundedSocketReader(UnboundedSocketSource source) {
      this.source = source;
    }

    private void openConnection() throws IOException {
      this.socket = new Socket();
      this.socket.connect(new InetSocketAddress(this.source.getHostname(), this.source.getPort()),
          CONNECTION_TIMEOUT_TIME);
      this.reader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
      this.isRunning = true;
    }

    @Override
    public boolean start() throws IOException {
      int attempt = 0;
      while (!isRunning) {
        try {
          openConnection();
          LOG.info("Connected to server socket " + this.source.getHostname() + ':'
              + this.source.getPort());

          return advance();
        } catch (IOException e) {
          LOG.info("Lost connection to server socket " + this.source.getHostname() + ':'
              + this.source.getPort() + ". Retrying in "
              + this.source.getDelayBetweenRetries() + " msecs...");

          if (this.source.getMaxNumRetries() == -1 || attempt++ < this.source.getMaxNumRetries()) {
            try {
              Thread.sleep(this.source.getDelayBetweenRetries());
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          } else {
            this.isRunning = false;
            break;
          }
        }
      }
      LOG.error("Unable to connect to host " + this.source.getHostname()
          + " : " + this.source.getPort());
      return false;
    }

    @Override
    public boolean advance() throws IOException {
      final StringBuilder buffer = new StringBuilder();
      int data;
      while (isRunning && (data = reader.read()) != -1) {
        // check if the string is complete
        if (data != this.source.getDelimiter()) {
          buffer.append((char) data);
        } else {
          if (buffer.length() > 0 && buffer.charAt(buffer.length() - 1) == '\r') {
            buffer.setLength(buffer.length() - 1);
          }
          this.currentRecord = buffer.toString();
          buffer.setLength(0);
          return true;
        }
      }
      return false;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      return new byte[0];
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return this.currentRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
      MetricsPusher.getInstance().pushMetrics();
      this.reader.close();
      this.socket.close();
      this.isRunning = false;
      LOG.info("Closed connection to server socket at " + this.source.getHostname() + ":"
          + this.source.getPort() + ".");
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return NOOP_CHECKPOINT_MARK;
    }

    @Override
    public UnboundedSource<String, ?> getCurrentSource() {
      return this.source;
    }
  }
}
