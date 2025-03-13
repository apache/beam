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
package org.apache.beam.sdk.io.pulsar;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;

@SuppressWarnings({"rawtypes"})
public class FakePulsarClient implements PulsarClient {

  private MockReaderBuilder readerBuilder;

  public FakePulsarClient(Reader<byte[]> reader) {
    this.setReader(reader);
  }

  @Override
  public ProducerBuilder<byte[]> newProducer() {
    return null;
  }

  @Override
  public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
    return null;
  }

  @Override
  public ConsumerBuilder<byte[]> newConsumer() {
    return null;
  }

  @Override
  public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
    return null;
  }

  public void setReader(Reader<byte[]> reader) {
    this.initReaderBuilder();
    readerBuilder.setReader(reader);
  }

  public void initReaderBuilder() {
    if (this.readerBuilder == null) {
      this.readerBuilder = new MockReaderBuilder();
    }
  }

  @Override
  public ReaderBuilder<byte[]> newReader() {
    this.initReaderBuilder();
    return this.readerBuilder;
  }

  @Override
  public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
    return null;
  }

  @Override
  public void updateServiceUrl(String serviceUrl) throws PulsarClientException {}

  public void serviceUrl(String serviceUrl) {}

  @Override
  public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
    return null;
  }

  @Override
  public void close() throws PulsarClientException {}

  @Override
  public CompletableFuture<Void> closeAsync() {
    return null;
  }

  @Override
  public void shutdown() throws PulsarClientException {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public TransactionBuilder newTransaction() throws PulsarClientException {
    return null;
  }

  static class MockReaderBuilder implements ReaderBuilder<byte[]> {

    private int numberOfMessages = 100;
    private String topic;
    private Reader<byte[]> reader;

    public MockReaderBuilder() {}

    public void setReader(Reader<byte[]> reader) {
      this.reader = reader;
    }

    @Override
    public Reader<byte[]> create() throws PulsarClientException {
      if (this.reader != null) {
        return this.reader;
      }
      this.reader = new FakePulsarReader(this.topic, this.numberOfMessages);
      return this.reader;
    }

    @Override
    public CompletableFuture<Reader<byte[]>> createAsync() {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> clone() {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> topic(String topicName) {
      this.topic = topicName;
      return this;
    }

    @Override
    public ReaderBuilder<byte[]> startMessageId(MessageId startMessageId) {
      return this;
    }

    @Override
    public ReaderBuilder<byte[]> startMessageFromRollbackDuration(
        long rollbackDuration, TimeUnit timeunit) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> startMessageIdInclusive() {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> readerListener(ReaderListener readerListener) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> defaultCryptoKeyReader(String privateKey) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> cryptoFailureAction(ConsumerCryptoFailureAction action) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> receiverQueueSize(int receiverQueueSize) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> readerName(String readerName) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> subscriptionRolePrefix(String subscriptionRolePrefix) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> subscriptionName(String subscriptionName) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> readCompacted(boolean readCompacted) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> keyHashRange(Range... ranges) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> defaultCryptoKeyReader(Map privateKeys) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> topics(List topicNames) {
      return null;
    }

    @Override
    public ReaderBuilder<byte[]> loadConf(Map config) {
      return null;
    }
  }
}
